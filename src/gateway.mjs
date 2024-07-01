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
}

const delay = timeout => new Promise(resolve => setTimeout(resolve, timeout));

export class MqttTimescaleGateway {
    #handlers;
    #transmitInterval;

    #sendQueue = [];
    #createdFields = new Set();

    constructor(handlers, transmitInterval) {
        this.#handlers = handlers;
        this.#transmitInterval = transmitInterval;
    }

    async run() {
        const dbClient = new Client();
        //const dbClient = { connect: () => { }, query: () => { } };

        await dbClient.connect();

        await this.#createTables(dbClient, Object.assign({}, ...this.#handlers.map(x => x.getTableTags())));

        const mqttClient = await this.#getMqttClient();

        mqttClient.on('message', (topic, message) => this.#onMessage(topic, message));

        await this.#handleSendQueue(dbClient);
    }

    async #createTables(dbClient, tableTagsByTable) {
        for (const table of Object.keys(tableTagsByTable)) {
            const tableTags = tableTagsByTable[table];
            const queryFormat = `
CREATE TABLE IF NOT EXISTS %I (
    ${['timestamp TIMESTAMPTZ NOT NULL', ...(tableTags.map(tag => format('%I TEXT NULL', tag)))].join(',\n    ')}
);
SELECT create_hypertable(%L, 'timestamp', if_not_exists => TRUE, CREATE_DEFAULT_INDEXES => FALSE);
CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (${['timestamp DESC', ...tableTags.map(tag => format('%I ASC', tag))].join(', ')});
                `;

            const query = format(queryFormat, table, table, table + '_tags_idx', table, tableTags)
            console.info(query);
            await dbClient.query(query);
        }
    }

    async #ensureFieldsExist(dbClient, table, fieldTypeMap) {
        const fieldsToCreate = Array.from(fieldTypeMap).filter(([columnName]) => !this.#createdFields.has(`${table}_${columnName}`));
        if (fieldsToCreate.length == 0)
            return;
        const queries = fieldsToCreate.map(([columnName, columnType]) => format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS %I %s NULL;', table, columnName, columnType)).join('\n');
        console.info(queries);
        await dbClient.query(queries);
        fieldsToCreate.forEach(([columnName]) => this.#createdFields.add(`${table}_${columnName}`));
    }

    async #send(dbClient, dataPoints) {
        if (dataPoints.length == 0)
            return;

        const byTable = Object.groupBy(dataPoints, x => x.table);

        for (const [table, pointsForTable] of Object.entries(byTable)) {
            // this abomination gets a map where the value name is the key, and the value type is the value
            let fieldTypeMap = new Map();
            pointsForTable.flatMap(point => Object.entries(point.values)).forEach(([valueName, value]) => fieldTypeMap.set(valueName, this.#getSqlType(value)));
            await this.#ensureFieldsExist(dbClient, table, fieldTypeMap);

            // this abomination groups by timestamp and all tags and merges the values of each group's points into a single object
            const byRow = Object.entries(Object.groupBy(pointsForTable, point => JSON.stringify({ timestamp: point.timestamp, tags: point.tags })))
                .map(([groupKey, groupPoints]) => ({ ...(JSON.parse(groupKey)), values: Object.assign({}, ...groupPoints.map(x => x.values)) }));

            for (const row of byRow) {
                const fieldsForRow = { ...row.tags, ...row.values };

                const columns = ['timestamp', ...Object.keys(fieldsForRow)];
                const valuesPlaceholders = columns.map((_, index) => index == 0 ? `to_timestamp($${index + 1})` : `$${index + 1}`);
                const valuesData = [row.timestamp / 1000, ...Object.values(fieldsForRow)];

                const query = format(`
                    INSERT INTO %I (%I)
                    VALUES (${valuesPlaceholders.join(', ')})
                    ON CONFLICT (%I) DO UPDATE SET ${columns.map((col) => format("%I = EXCLUDED.%I", col, col)).join(', ')};`, table, columns, ['timestamp', ...Object.keys(row.tags)]);

                console.info(query, valuesData);
                await dbClient.query(query, valuesData);
            }
        }
    }

    #getSqlType(val) {
        switch (typeof val) {
            case "string":
                return 'TEXT';
            case "number":
                return 'DOUBLE PRECISION';
            default:
                throw `Invalid type ${typeof val}`;
        }
    }

    async #getMqttClient() {
        const mqttClient = await mqtt.connectAsync(MQTT_SERVER, { username: MQTT_USER, password: MQTT_PASSWORD });

        mqttClient.on('error', x => {
            console.error(x);
        });

        console.info('connected');
        mqttClient.subscribe(this.#handlers.flatMap(x => x.getMqttTopics()));
        return mqttClient;
    }

    #onMessage(topic, message) {
        const matchingHandlers = this.#handlers.filter(handler => handler.getMqttTopics().some(handlerTopic => mqttWildcard(topic, handlerTopic)));

        for (const handler of matchingHandlers) {
            const points = handler.getDataPointsFromMqttMessage(topic.split('/'), message.toString());
            this.#sendQueue.push(...points);
        }
    }

    async #handleSendQueue(dbClient) {
        while (true) {
            try {
                const sendQueueItems = [...this.#sendQueue];

                await this.#send(dbClient, sendQueueItems);

                this.#sendQueue.splice(0, sendQueueItems.length);
            } catch (e) {
                console.error(e);
            }
            await delay(this.#transmitInterval);
        }
    }
}
