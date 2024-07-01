# mqtt-timescale-gateway
Configurable gateway for pushing data from MQTT to TimescaleDB

Features:
- freely configurable by writing message handlers 
    - easily do raw values, JSON, value aggregation/transformation, etc.
    - multiple different data topics/devices/device types per gateway instance
- no manual schema creation necessary
    - automatic creation of tables and indices (tags added afterwards have to be added to table and index manually though)
    - automatic expansion of table columns

**Warning**: While I took care to avoid the possibility of SQL injection, I cannot guarantee this is 100% safe, as the queries can't be fully parameterized (due to the dynamic table/fiels definitions), so you probably shouldn't use this with any untrusted/public MQTT server.

## Examples

Simple handler for MQTT float values:
```js
import { MqttHandler } from "./gateway.mjs"

class PowerSensorHandler extends MqttHandler {
    // example: power/sensor01/watts 123.45

    // get list of topics to listen to
    getMqttTopics() {
        return ['power/+/watts'];
    }

    getDataPointsFromMqttMessage(splitTopic, message) {
        const sensorId = splitTopic[1];
        const valueName = splitTopic[2];

        let values = {};
        values[valueName] = parseFloat(message);

        return [{ table: 'power', tags: {sensor_id: sensorId}, values: values, timestamp: Date.now() }];
        // tag values are always strings and get added to the db index
        // field values can be number (-> DOUBLE PRECISION), string (-> TEXT) or boolean (-> BOOLEAN)
    }

    // get the list of tags for each table (for table and index creation)
    getTableTags() {
        return {
            power: ['sensor_id']
        };
    }
}
```

Simple handler for MQTT JSON values:
```js
import { MqttHandler } from "./gateway.mjs"

class ClimateSensorHandler extends MqttHandler {
    // example: climate/sensor01/values { "temperature": 20.05, "humidity": 71.2 }

    getMqttTopics() {
        return ['climate/+/values'];
    }

    getDataPointsFromMqttMessage(splitTopic, message) {
        const sensorId = splitTopic[1];

        let values = JSON.parse(message);

        return [{ table: 'climate', tags: {sensor_id: sensorId}, values: values, timestamp: Date.now() }];
    }

    getTableTags() {
        return {
            climate: ['sensor_id']
        };
    }
}
```

A more complex handler example which tries to correlate/merge values by their timestamp can be found in `src/index.example.mjs`.