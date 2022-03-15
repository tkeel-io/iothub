# python 3
import random
import time

from paho.mqtt import client as mqtt_client

broker = '192.168.123.9'
port = 31883

# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
# 设备ID
username = 'iotd-ff565551-7c86-44b9-b2e9-504806e04cba'
# 设备token
password = "YTVkMGU5YTctZjYxNi0zYmRhLThjM2EtYmJkYmNiZmJmYTcy"


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


def run():
    client = connect_mqtt()
    client.loop_start()
    time.sleep(5)


if __name__ == '__main__':
    run()
