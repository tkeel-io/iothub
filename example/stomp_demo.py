# coding = utf-8
# pip install stomp.py

import time
import stomp
import json


class MyListener(object):

    def on_error(self, frame):
        print('received an error %s' % frame)

    def on_message(self, frame):
        print('received a message %s' % frame)

        def on_disconnected(self):
            """
            Increment the disconnect count. See :py:meth:`ConnectionListener.on_disconnected`
            """
        print("disconnected (x %s)")

    def on_connecting(self, host_and_port):
        """
        Increment the connection count. See :py:meth:`ConnectionListener.on_connecting`

        :param (str,int) host_and_port: the host and port as a tuple
        """
        print("connecting ", host_and_port[0], host_and_port[1])

    def on_send(self, frame):
        """
        Increment the send count. See :py:meth:`ConnectionListener.on_send`

        :param Frame frame:
        """
        print("send %s", frame)

    def on_heartbeat_timeout(self):
        """
        Increment the heartbeat timeout. See :py:meth:`ConnectionListener.on_heartbeat_timeout`
        """
        print("received heartbeat timeout")

    def on_heartbeat(self):
        """
        Increment the heartbeat count. See :py:meth:`ConnectionListener.on_heartbeat`
        """
        print("on_heartbeat ...")


conn = stomp.Connection([('192.168.123.9', 31613)])
conn.set_listener('', MyListener())
username = 'jesse'
password = "123456"
conn.connect(username, password, wait=True)
# conn.connect()
conn.subscribe(destination='/queue/test', id=1, ack='auto')

data_attributes = {
    "attribute1": "value1",
    "attribute2": 1234
}

try:
    while True:
        conn.send(body=json.dumps(data_attributes), destination='/queue/test', content_type="application/json")
        time.sleep(3)
except Exception as e:
    print("except %s", e.__str__())
    conn.disconnect()
