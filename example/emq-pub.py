# python 3.6

import random
import time
import json

from paho.mqtt import client as mqtt_client


broker = '192.168.123.9'
port = 31136
topic_attr = "v1/devices/me/attributes"
topic = "v1/devices/me/telemetry"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = '1cb1750c-2b95-4f0b-9a38-43cfb6b13418'
password = "NDgzMDY5MGItNjMyMy0zN2ZlLWIwZmUtMjEzNzFmNWFkZjY0"


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
        time.sleep(2)
        # msg = f"messages: {msg_count}"
        attr = {
            "attribute1": "value1",
            "attribute2": msg_count
        }
        msg = {
            "ts": time.time_ns(),
            "values": {
                "telemetry1": "value1",
                "telemetry2": msg_count
            }
        }
        if msg_count % 2 == 1:
            result = client.publish(topic_attr, json.dumps(attr))
        else:
            result = client.publish(topic, json.dumps(msg))
        # result: [0, 1]
        status = result[0]
        if status == 0:
            if msg_count % 2 == 1:
                print(f"Send `{attr}` to topic `{topic_attr}`")
            else:
                print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
        msg_count += 1


def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)


if __name__ == '__main__':
    run()
