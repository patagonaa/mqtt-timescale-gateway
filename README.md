# mqtt-timescale-gateway
Configurable gateway for pushing data from MQTT to TimescaleDB

Features:
- freely configurable by writing message handlers
- automatic creation and expansion of database tables

Warning: While I took great care to avoid the possibility of SQL injection, I cannot guarantee this is 100% safe, as the queries can't be fully parameterized, so you probably shouldn't use this on any public MQTT server.