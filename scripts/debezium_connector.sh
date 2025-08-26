#!/bin/bash

curl -i -X POST -H "Accept:application/json" \
     -H "Content-Type:application/json" localhost:8083/connectors/ \
     -d '{
          "name": "iot-events-connector",
          "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": "offline-ds",
                "database.port": "5432",
                "database.user": "postgres",
                "database.password": "mysecretpassword",
                "database.dbname": "iot_db",
                "database.server.name": "iot",
                "table.include.list": "public.iot_events",
                "plugin.name": "pgoutput",
                "slot.name": "iot_raw_data_slot"
            }
        }'