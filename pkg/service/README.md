# 会话实体

创建 会话实体 动态维护与平台建立连接的信息（protocol, 订阅的 topics， 订阅实体）, 区别于没有没有建立连接的设备实体。
在 iothub 初始化的时候需要加载这些信息， 避免因为 iothub 重启丢失这些连接信息。

针对 MQTT、CoAP 具体实现逻辑：
1. OnClientConnected --> 创建会话实体（id, protocol, clientid, sub-topics， 设备ID）

2. OnClientSubscribe --> 更新会话实体（sub-topics， 创建订阅实体（订阅属性遥测变更topic））

3. OnClientUnsubscribe --> 更新会话实体（sub-topics，删除订阅实体）

4. OnClientDisconnected --> 删除会话实体（id） 订阅实体（id）



options:
option1: 实现将会话实体(连接信息)， 订阅实体保存在 core db 或者状态存储 
option2: 通过 API 从 emqx 获取(不能获取pw)






get clients info
```bash
curl --location --request GET 'http://192.168.100.5:30855/api/v4/clients?_page=1&_limit=10&_=1640597122737' \
--header 'Authorization: Basic YWRtaW46cHVibGlj'
```

>>>
```json
{
    "meta": {
        "page": 1,
        "limit": 10,
        "hasnext": false,
        "count": 1
    },
    "data": [
        {
            "recv_cnt": 11,
            "heap_size": 987,
            "zone": "external",
            "proto_ver": 4,
            "node": "iothub@emqx-0.emqx-headless.keel-system.svc.cluster.local",
            "send_pkt": 11,
            "send_oct": 24,
            "max_awaiting_rel": 100,
            "max_inflight": 32,
            "clientid": "trest",
            "created_at": "2021-12-27 11:55:51",
            "expiry_interval": 7200,
            "proto_name": "MQTT",
            "send_msg": 0,
            "recv_msg": 0,
            "max_mqueue": 1000,
            "reductions": 12226,
            "awaiting_rel": 0,
            "recv_oct": 96,
            "username": "mqttx",
            "mqueue_len": 0,
            "port": 20886,
            "subscriptions_cnt": 0,
            "ip_address": "10.10.137.64",
            "is_bridge": false,
            "max_subscriptions": 0,
            "mountpoint": "mqttx/",
            "keepalive": 60,
            "connected_at": "2021-12-27 11:55:51",
            "inflight": 0,
            "mqueue_dropped": 0,
            "connected": true,
            "send_cnt": 11,
            "recv_pkt": 11,
            "mailbox_len": 0,
            "clean_start": false
        }
    ],
    "code": 0
}
```



get sub topics info
```bash
curl --location --request GET 'http://192.168.100.5:30855/api/v4/subscriptions?_page=1&_limit=1000' \
--header 'Authorization: Basic YWRtaW46cHVibGlj'
```

```json
{
    "meta": {
        "page": 1,
        "limit": 1000,
        "hasnext": false,
        "count": 2
    },
    "data": [
        {
            "topic": "mqttx/test/#",
            "qos": 0,
            "node": "iothub@emqx-0.emqx-headless.keel-system.svc.cluster.local",
            "clientid": "trest"
        },
        {
            "topic": "mqttx/testtopic/#",
            "qos": 0,
            "node": "iothub@emqx-0.emqx-headless.keel-system.svc.cluster.local",
            "clientid": "trest"
        }
    ],
    "code": 0
}
```



publish to emq
```bash
curl --location --request POST 'http://192.168.100.5:30855/api/v4/mqtt/publish' \
--header 'Authorization: Basic YWRtaW46cHVibGlj' \
--header 'Content-Type: application/json' \
--data-raw '{
    "topic":"71e7ce32-dc94-4c44-af95-e16002388993/v1/devices/me/attributes",
    "payload":"Hello World",
    "qos":1,
    "retain":false,
    "clientid":"python-mqtt-34"
}'
```

resp
```json
{
  "code": 0
}
```



## 获取 core 的实体信息
```bash
curl --location --request GET 'http://192.168.100.5:30707/apis/core/v1/entities/71e7ce32-dc94-4c44-af95-e16002388993?type=device&owner=usr-1-7af89d4a8d512740053ea07c000aa143&source=device' --header 'Authorization: Bearer eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiIwMDAwMDAiLCJleHAiOjE2NDM1NzExNzYsInN1YiI6InVzci0xLTdhZjg5ZDRhOGQ1MTI3NDAwNTNlYTA3YzAwMGFhMTQzIn0.Yvgh75Jn-WHwnzPcIC1rKh35QyHjt21ru3pvuQeqGFQxi1nGIuOuUBHuVQJE43d6brD4CfWUttU_jF2QVP4jJw' | jq '.'
```

```json
{
  "id": "71e7ce32-dc94-4c44-af95-e16002388993",
  "source": "device",
  "owner": "usr-1-7af89d4a8d512740053ea07c000aa143",
  "type": "device",
  "configs": {},
  "properties": {
    "attributes": {
      "data": "",
      "timestamp": 1640783576857733000,
      "topic": "71e7ce32-dc94-4c44-af95-e16002388993/v1/devices/me/attributes"
    },
    "connectinfo": {
      "_clientid": "trest2",
      "_online": true,
      "_owner": "",
      "_peerhost": "10.10.137.64",
      "_protocol": "mqtt",
      "_socketport": "1883",
      "_username": "71e7ce32-dc94-4c44-af95-e16002388993"
    },
    "dev": {
      "desc": "dev_desc",
      "ext": {
        "other": "other",
        "version": "1.1"
      },
      "group": "default",
      "name": "dev_name"
    },
    "rawData": {
      "data": {
        "msg": "iothub"
      },
      "timestamp": 1640783468189722000,
      "topic": "71e7ce32-dc94-4c44-af95-e16002388993/v1/devices/me/raw"
    },
    "sysField": {
      "_createdAt": 1640679963551042300,
      "_enable": true,
      "_id": "71e7ce32-dc94-4c44-af95-e16002388993",
      "_token": "NDGXZJI1NTUTMTNMMS0ZZMM0LWIWMTUTZDG1MDI3NGNKN2MY",
      "_updatedAt": 1640679963551042300
    }
  }
}
```

