# python 3.6

import random
import time
import json

from paho.mqtt import client as mqtt_client


broker = '192.168.123.9'
port = 31136
topic_attributes = "v1/devices/me/attributes"
topic_telemetry = "v1/devices/me/telemetry"
topic_attributes_gateway = "v1/gateway/attributes"
topic_telemetry_gateway = "v1/gateway/telemetry"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = 'a8e92c6d-0f73-4f7a-8b85-0f110155eed2'
password = "NWExMTg3NTUtZWVhNS0zYzNiLWEzNmEtODIzMDU2MWFkMWM1"


def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client):
    msg_count = 0
    while True:
        time.sleep(5)
        # msg = f"messages: {msg_count}"
        data_attributes = {
            "attribute1": "value1",
            "attribute2": msg_count
        }
        data_telemetry = {
            "ts": time.time_ns(),
            "values": {
                "telemetry1": "value1",
                "telemetry2": msg_count
            }
        }
        data_attributes_gateway = {
            "devA": {
                "attribute1": "value1",
                "telemetry2": msg_count
            },
            "devB": {
                "attribute1": "value1",
                "telemetry2": msg_count
            }
        }
        data_telemetry_gateway = {
            "devA": [
                {
                    "ts": time.time_ns(),
                    "values": {
                        "telemetry1": "value1",
                        "telemetry2": msg_count
                    }
                },
            ]
        }
        if msg_count % 5 == 0:
            topic = topic_attributes
            payload = data_attributes
        elif msg_count % 5 == 1:
            topic = topic_telemetry
            payload = data_telemetry
        elif msg_count % 5 == 2:
            topic = topic_attributes_gateway
            payload = data_attributes_gateway
        elif msg_count % 5 == 3:
            topic = topic_telemetry_gateway
            payload = data_telemetry_gateway
        else:
            topic = "xxx/v1/user/define"
            payload = bytes("dddddddddd", encoding="utf-8")
        if isinstance(payload, (str, bytes, bytearray)):
            result = client.publish(topic, payload)
        else:
            result = client.publish(topic, json.dumps(payload))
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{payload}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
        msg_count += 1
        # msg_count = 4


def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)


if __name__ == '__main__':
    run()
