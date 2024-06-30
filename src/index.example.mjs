import { MqttHandler, MqttTimescaleGateway } from "./gateway.mjs"

const transmitInterval = 2500; // (ms)
const timeGroupInterval = 1000; // (ms) group all values received in this timeframe into one timestamp


class ModbusPowermeterHandler extends MqttHandler {
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

    getDataPointsFromMqttMessage(splitTopic, message) {
        const clientId = splitTopic[1];
        const sensorId = splitTopic[3];

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

        this.#lastTimestamp = timestamp;

        return [{ table: table, tags: tags, values: values, timestamp: timestamp }];
    }

    // get the list of tags for each table (for table and index creation)
    getTableTags() {
        return {
            modbus_powermeter: ['client_id'],
            modbus_powermeter_by_phase: ['client_id', 'phase']
        };
    }
}

const handlers = [new ModbusPowermeterHandler(timeGroupInterval)];

new MqttTimescaleGateway(handlers, transmitInterval).run();
