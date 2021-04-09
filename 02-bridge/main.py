#!/usr/bin/env python3

"""A MQTT to InfluxDB Bridge

This script receives MQTT data and saves those to InfluxDB.

"""

import datetime
import re
import time
from typing import NamedTuple
import json

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

INFLUXDB_ADDRESS = 'influxdb'
INFLUXDB_USER = 'telegraf'
INFLUXDB_PASSWORD = 'telegraf'
INFLUXDB_DATABASE = 'sensors'

MQTT_ADDRESS = 'mosquitto'
MQTT_USER = 'mqtt_user'
MQTT_PASSWORD = 'mqtt_pass'
#MQTT_TOPIC = 'home/+/+'  # [bme280|mijia]/[temperature|humidity|battery|status]
MQTT_TOPIC = 'sensors/tele/+/SENSOR'
MQTT_REGEX = 'sensors/tele/([^/]+)/SENSOR'
MQTT_CLIENT_ID = 'MQTTInfluxDBBridge'

influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None)


class SensorData(NamedTuple):
    measurement: str
    tags: dict
    fields: dict
    timestamp: int


def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    print(msg.topic + ' ' + str(msg.payload))
    _parse_mqtt_message(msg.topic, json.loads(msg.payload))       


def _parse_mqtt_message(topic, payload):
    tags={}
    fields={}
    sensors=[]
    match = re.match(MQTT_REGEX, topic)
    if match:
        tags['location'] = match.group(1)
        #measurement = match.group(2)

        for item in payload:
            if item == 'Time':
                timestamp = int(time.mktime(datetime.datetime.strptime(payload[item],'%Y-%m-%dT%H:%M:%S').timetuple()))
            elif type(payload[item]) is dict:
                sensors.append(item)
            else:
                tags[item]= payload[item]
        
        for s in sensors:
            tags['sensor'] = s
            for item in payload[s]:
                if item == 'Id':
                    tags[item]= payload[s][item]
                else:
                    measurement = item
                    fields['value'] = float(payload[s][item])
            sensor_data = SensorData(measurement, tags, fields, timestamp)
            _send_sensor_data_to_influxdb(sensor_data)
            print(sensor_data)
    else:
        return None


def _send_sensor_data_to_influxdb(sensor_data):
    json_body = [
        {
            'measurement': sensor_data.measurement,
            'tags': sensor_data.tags,
            'fields': sensor_data.fields,
            'timestamp': sensor_data.timestamp
        }
    ]
    influxdb_client.write_points(json_body, time_precision='s')
    #print(json_body)


def _init_influxdb_database():
    databases = influxdb_client.get_list_database()
    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        influxdb_client.create_database(INFLUXDB_DATABASE)
    influxdb_client.switch_database(INFLUXDB_DATABASE)


def main():
    _init_influxdb_database()

    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    mqtt_client.loop_forever()


if __name__ == '__main__':
    print('MQTT to InfluxDB bridge')
    main()
