import { MqttHandler, MqttTimescaleGateway } from "./gateway.mjs"

const transmitInterval = 2500; // (ms)
const timeGroupInterval = 1000; // (ms) group all values received in this timeframe into one timestamp

class JanitzaJsonHandler extends MqttHandler {
    // for use with https://github.com/LeoDJ/Janitza2Influx

    getMqttTopics() {
        return ['power/janitza/+/json'];
    }
    getTableTags() {
        return {
            janitza_all: ['SerialNumber'],
            janitza_per_phase: ['SerialNumber', 'Phase'],
            janitza_betweeen_phases: ['SerialNumber', 'Phases']
        };
    }
    getDataPointsFromMqttMessage(splitTopic, message) {
        const data = JSON.parse(message);

        const tableName = 'janitza_all';
        const tableNamePerPhase = 'janitza_per_phase';
        const tableNameBetweenPhases = 'janitza_betweeen_phases';

        const timestamp = Date.now();
        const serialNumber = data.SerialNumber.toString();

        let valuesAll = {};
        let valuesPerPhase = {};
        let valuesBetweenPhases = {};

        for (const [valueKey, valueData] of Object.entries(data.Values)) {
            if (valueData == null || (typeof valueData) == "number") {
                valuesAll[valueKey] = valueData;
            } else if ((typeof valueData) == "object") {
                for (const [phase, value] of Object.entries(valueData)) {
                    if (phase == 'All') {
                        valuesAll[valueKey] = value;
                    } else if (/L\d-L\d/.test(phase)) {
                        let values = valuesBetweenPhases[phase] ??= {};
                        values[valueKey] = value;
                    } else {
                        let values = valuesPerPhase[phase] ??= {};
                        values[valueKey] = value;
                    }
                }
            }
        }

        let dataPoints = [];

        dataPoints.push({ table: tableName, tags: { SerialNumber: serialNumber }, values: valuesAll, timestamp: timestamp });
        for (const [phase, values] of Object.entries(valuesPerPhase)) {
            dataPoints.push({ table: tableNamePerPhase, tags: { SerialNumber: serialNumber, Phase: phase }, values: values, timestamp: timestamp });
        }
        for (const [phases, values] of Object.entries(valuesBetweenPhases)) {
            dataPoints.push({ table: tableNameBetweenPhases, tags: { SerialNumber: serialNumber, Phases: phases }, values: values, timestamp: timestamp });
        }

        return dataPoints;
    }
}

class ModbusPowermeterHandler extends MqttHandler {
    // example for use with https://gist.github.com/patagonaa/a40529d352873377f352fa2c97266f8f
    // examples: 
    // modbus_powermeter/patagona-orno-01/sensor/voltage_l1/state 239.3
    // modbus_powermeter/patagona-orno-01/sensor/active_power_total/state 77
    // modbus_powermeter/patagona-orno-01/sensor/grid_frequency/state 50.02

    #lastTimestamp = 0;
    #timeGroupInterval;

    constructor(timeGroupInterval) {
        super();
        this.#timeGroupInterval = timeGroupInterval;
    }

    // get list of topics to listen to
    getMqttTopics() {
        return ['modbus_powermeter/+/sensor/+/state'];
    }

    getDataPointsFromMqttMessage(splitTopic, message, packet) {
        const clientId = splitTopic[1];

        const time = Date.now();

        let timestamp;
        if (time > (this.#lastTimestamp + this.#timeGroupInterval)) {
            timestamp = time;
        } else {
            timestamp = this.#lastTimestamp;
        }

        let table;
        let tags = { client_id: clientId };
        let values = {};

        if (splitTopic[2] == 'status') {
            table = 'modbus_powermeter_status';
            values['status'] = (message == 'online');
        } else if (splitTopic[2] == 'sensor') {
            if (packet.retain)
                return [];

            const sensorId = splitTopic[3];

            if (sensorId.endsWith('_l1') || sensorId.endsWith('_l2') || sensorId.endsWith('_l3')) {
                let sensorSplit = sensorId.split('_');
                const phase = sensorSplit.pop().toUpperCase();
                const measurement = sensorSplit.join('_');

                table = 'modbus_powermeter_by_phase';
                tags['phase'] = phase;
                values[measurement] = parseFloat(message);
            } else {
                table = 'modbus_powermeter';
                values[sensorId] = parseFloat(message);
            }
            if (Object.values(values).some(x => !isFinite(x))) {
                console.error('Invalid value', splitTopic.join('/'), message)
                return [];
            }
        } else {
            return [];
        }

        this.#lastTimestamp = timestamp;

        return [{ table: table, tags: tags, values: values, timestamp: timestamp }];
    }

    // get the list of tags for each table (for table and index creation)
    getTableTags() {
        return {
            modbus_powermeter_status: ['client_id'],
            modbus_powermeter: ['client_id'],
            modbus_powermeter_by_phase: ['client_id', 'phase']
        };
    }
}

const handlers = [new ModbusPowermeterHandler(timeGroupInterval)];

new MqttTimescaleGateway(handlers, transmitInterval).run();
