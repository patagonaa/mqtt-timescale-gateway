import * as mqtt from "mqtt";
import pg from 'pg'
import mqttWildcard from "mqtt-wildcard";
import format from "pg-format";
const { Client } = pg

const MQTT_SERVER = process.env.MQTT_SERVER || "mqtt://localhost";
const MQTT_USER = process.env.MQTT_USER;
const MQTT_PASSWORD = process.env.MQTT_PASSWORD;

export class MqttHandler {
    getMqttTopics() {
        throw 'Not implemented';
    }
    getDataPointsFromMqttMessage(splitTopic, message) {
        throw 'Not implemented';
    }
    getTableTags() {
        throw 'Not implemented';
    }
}

const delay = timeout => new Promise(resolve => setTimeout(resolve, timeout));


class TimescaleDataSender {
    #dbClient;
    #logQueries;
    #createdFields = new Set();

    constructor(dbClient, logQueries) {
        this.#dbClient = dbClient;
        this.#logQueries = logQueries;
    }

    static async create(logQueries) {
        const dbClient = new Client();
        //const dbClient = { connect: () => { }, query: () => { } };
        await dbClient.connect();
        console.info('DB connected');
        return new TimescaleDataSender(dbClient, logQueries);
    }

    async createTables(tableTagsByTable) {
        for (const table of Object.keys(tableTagsByTable)) {
            const tableTags = tableTagsByTable[table];
            const queryFormat = `
CREATE TABLE IF NOT EXISTS %I (
    ${['timestamp TIMESTAMPTZ NOT NULL', ...(tableTags.map(tag => format('%I TEXT NULL', tag)))].join(',\n    ')}
);
SELECT create_hypertable(%L, 'timestamp', if_not_exists => TRUE, CREATE_DEFAULT_INDEXES => FALSE);
CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (${['timestamp DESC', ...tableTags.map(tag => format('%I ASC', tag))].join(', ')});`;

            const query = format(queryFormat, table, table, table + '_tags_idx', table, tableTags)
            if (this.#logQueries)
                console.debug(query);
            await this.#dbClient.query(query);
        }
    }

    async #ensureFieldsExist(table, fieldTypeMap) {
        const fieldsToCreate = Array.from(fieldTypeMap).filter(([columnName]) => !this.#createdFields.has(`${table}_${columnName}`));
        if (fieldsToCreate.length == 0)
            return;
        const queries = fieldsToCreate.map(([columnName, columnType]) => format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS %I %s NULL;', table, columnName, columnType)).join('\n');
        if (this.#logQueries)
            console.debug(queries);
        await this.#dbClient.query(queries);
        fieldsToCreate.forEach(([columnName]) => this.#createdFields.add(`${table}_${columnName}`));
    }

    async send(dataPoints) {
        if (dataPoints.length == 0)
            return;

        const byTable = Object.groupBy(dataPoints, x => x.table);

        for (const [table, pointsForTable] of Object.entries(byTable)) {
            // this abomination gets a map where the value name is the key, and the value type is the value
            let fieldTypeMap = new Map();
            pointsForTable
                .flatMap(point => Object.entries(point.values)) // get [valueName, value] for all points
                .filter(([_, value]) => value != null) // filter out null values because we can't infer a type from that
                .forEach(([valueName, value]) => fieldTypeMap.set(valueName, this.#getSqlType(value, valueName)));
            await this.#ensureFieldsExist(table, fieldTypeMap);

            pointsForTable.flatMap(point => Object.entries(point.tags)).forEach(([tagName, tagValue]) => { if ((typeof tagValue) != 'string') throw `Invalid type ${typeof tagValue} in tag ${tagName}`; });

            // this abomination groups by timestamp and all tags and merges the values of each group's points into a single object
            const byRow = Object.entries(Object.groupBy(pointsForTable, point => JSON.stringify({ timestamp: point.timestamp, tags: point.tags })))
                .map(([groupKey, groupPoints]) => ({ ...(JSON.parse(groupKey)), values: Object.assign({}, ...groupPoints.map(x => x.values)) }));

            for (const row of byRow) {
                const fieldsForRow = { ...row.tags, ...row.values };

                for (const [field, fieldValue] of Object.entries(fieldsForRow)) {
                    if (fieldValue == null)
                        delete fieldsForRow[field]; // do not explicitly insert null values so columns that haven't been added yet don't cause an error
                }

                const columns = ['timestamp', ...Object.keys(fieldsForRow)];
                const valuesPlaceholders = columns.map((_, index) => index == 0 ? `to_timestamp($${index + 1})` : `$${index + 1}`);
                const valuesData = [row.timestamp / 1000, ...Object.values(fieldsForRow)];

                const query = format(`
INSERT INTO %I (%I)
VALUES (${valuesPlaceholders.join(', ')})
ON CONFLICT (%I) DO UPDATE SET ${columns.map((col) => format("%I = EXCLUDED.%I", col, col)).join(', ')};`, table, columns, ['timestamp', ...Object.keys(row.tags)]);

                if (this.#logQueries)
                    console.debug(query, valuesData);
                await this.#dbClient.query(query, valuesData);
            }
        }
    }

    #getSqlType(val, valueName) {
        switch (typeof val) {
            case "string":
                return 'TEXT';
            case "number":
                return 'DOUBLE PRECISION';
            case "boolean":
                return 'BOOLEAN'
            default:
                throw `Invalid type ${typeof val} in field ${valueName}`;
        }
    }
}

export class MqttTimescaleGateway {
    #handlers;
    #transmitInterval;

    #sendQueue = [];

    constructor(handlers, transmitInterval) {
        this.#handlers = handlers;
        this.#transmitInterval = transmitInterval;
    }

    async run() {
        const dataSender = await TimescaleDataSender.create(true);

        await dataSender.createTables(Object.assign({}, ...this.#handlers.map(x => x.getTableTags())));

        const mqttClient = await this.#getMqttClient();

        mqttClient.on('message', (topic, message) => this.#onMessage(topic, message));

        await this.#handleSendQueue(dataSender);
    }


    async #getMqttClient() {
        const mqttClient = await mqtt.connectAsync(MQTT_SERVER, { username: MQTT_USER, password: MQTT_PASSWORD });

        mqttClient.on('error', x => {
            console.error('MQTT error', x);
        });

        console.info('MQTT connected');
        mqttClient.subscribe(this.#handlers.flatMap(x => x.getMqttTopics()));
        return mqttClient;
    }

    #onMessage(topic, message) {
        const matchingHandlers = this.#handlers.filter(handler => handler.getMqttTopics().some(handlerTopic => mqttWildcard(topic, handlerTopic)));

        for (const handler of matchingHandlers) {
            try {
                const points = handler.getDataPointsFromMqttMessage(topic.split('/'), message.toString());
                this.#sendQueue.push(...points);
            } catch (error) {
                console.error('handler failed!', error);
            }
        }
    }

    async #handleSendQueue(dataSender) {
        while (true) {
            try {
                const sendQueueItems = [...this.#sendQueue];

                await dataSender.send(sendQueueItems);
                if (sendQueueItems.length > 0)
                    console.info('successfully transmitted', sendQueueItems.length, 'values');

                this.#sendQueue.splice(0, sendQueueItems.length);
            } catch (e) {
                console.error(e);
            }
            await delay(this.#transmitInterval);
        }
    }
}
