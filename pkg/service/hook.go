package service

import (
    "context"
    "crypto/md5"
    "encoding/base64"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "net/http"
    "os"
    "reflect"
    "strconv"
    "strings"
    "time"

    "github.com/Shopify/sarama"
    cloudevents "github.com/cloudevents/sdk-go/v2"
    dapr "github.com/dapr/go-sdk/client"
    "github.com/google/uuid"
    jsoniter "github.com/json-iterator/go"
    "github.com/pkg/errors"
    "github.com/prometheus/client_golang/prometheus"
    v1 "github.com/tkeel-io/core/api/core/v1"
    pb "github.com/tkeel-io/iothub/protobuf"
    "github.com/tkeel-io/kit/log"
)

const (
    //state store
    iothubPrivateStatesStoreName = `iothub-private-store`
    connectInfoSuffixKey         = `_ci`
    devEntitySuffixKey           = `_de`
    subEntitySuffixKey           = `_sub`

    //different properties of device entity
    rawDataProperty     = `rawData`
    attributeProperty   = `attributes`
    telemetryProperty   = `telemetry`
    commandProperty     = `commands`
    connectInfoProperty = `connectInfo`
    rawDownProperty     = `rawDown`

    // mark
    MarkUpStream   = "upstream"
    MarkDownStream = "downstream"
    MarkConnecting = "connecting"

    // subscription mode
    onChangeMode = "onChange"
    realtimeMode = "realtime"

    // default client id for cloud
    defaultDownStreamClientId = `@tkeel.iothub.internal.clientId`

    // default header key
    tkeelAuthHeader = `x-tKeel-auth`
    defaultTenant   = `_tKeel_system`
    defaultUser     = `_tKeel_admin`
    defultRole      = `admin`

    // default base url
    BaseUrl = `http://localhost:3500/v1.0/invoke/keel/method/apis`
)
const (
    _envCorePubTopic = `CORE_PUB_TOPIC`
    _envKafkaService = `KAFKA_SERVICE`
)

func propertyTypeFromTopic(topic string) string {
    switch topic {
    case AttributesTopic:
        return attributeProperty
    case TelemetryTopic:
        return telemetryProperty
    case CommandTopicResponse:
        return commandProperty
    }
    return ""
}

// HookService is used to implement emqx_exhook_v1.s *HookService.
type HookService struct {
    pb.UnimplementedHookProviderServer
    daprClient dapr.Client
    //clients map[string]*ConnectInfo
    //entities map[string]*DeviceEntityInfo

    // map["clientid"][{"topic": "xxx/xxx", "qos": 0, "node": "XXX"},]
    subscribeTopics map[string][]map[string]interface{}
    producer        sarama.AsyncProducer
    // the topic pub to core
    corePubTopic string
    // metrics
    collector *Collector
}

type Collector struct {
    msgTotal *prometheus.CounterVec
    devTotal *prometheus.GaugeVec
}

func NewHookService(client dapr.Client) *HookService {
    config := sarama.NewConfig()
    config.Producer.Return.Successes = true
    config.Producer.Return.Errors = true
    ad := envWithDefault(_envKafkaService, `kafka.keel-system.svc.cluster.local:9092`)
    ads := strings.Split(ad, ";")
    p, err := sarama.NewAsyncProducer(ads, config)
    // TODO: error
    if err != nil {
        panic("error")
    }
    go func(p sarama.AsyncProducer) {
        errs := p.Errors()
        success := p.Successes()
        for {
            select {
            case rc := <-errs:
                if rc != nil {
                    // TODO:
                }
                return
            case _ = <-success:
            }
        }
    }(p)
    //
    msgReq := prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "iothub_msg_total",
            Help: "How many msg requests processed, partitioned by direction and tenant.",
        },
        []string{"tenant", "direction"},
    )
    //
    prometheus.MustRegister(msgReq)
    //
    devTotal := prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Subsystem: "runtime",
            Name:      "dev_online_total",
            Help:      "Number of goroutines that currently exist.",
        },
        []string{"tenant"},
    )
    prometheus.MustRegister(devTotal)
    // create metrics
    mc := &Collector{
        msgTotal: msgReq,
        devTotal: devTotal,
    }
    //
    return &HookService{
        daprClient:   client,
        producer:     p,
        corePubTopic: envWithDefault(_envCorePubTopic, "core-pub"),
        collector:    mc,
    }
}

//
func envWithDefault(envVal, defaultVal string) string {
    if s := os.Getenv(envVal); s != "" {
        return s
    }
    return defaultVal
}

// HookProviderServer callbacks

func (s *HookService) OnProviderLoaded(ctx context.Context, in *pb.ProviderLoadedRequest) (*pb.LoadedResponse, error) {
    hooks := []*pb.HookSpec{
        {Name: "client.connect"},
        {Name: "client.connack"},
        {Name: "client.connected"},
        {Name: "client.disconnected"},
        {Name: "client.authenticate"},
        {Name: "client.check_acl"},
        {Name: "client.subscribe"},
        {Name: "client.unsubscribe"},
        {Name: "session.created"},
        {Name: "session.subscribed"},
        {Name: "session.unsubscribed"},
        {Name: "session.resumed"},
        {Name: "session.discarded"},
        {Name: "session.takeovered"},
        {Name: "session.terminated"},
        {Name: "message.publish"},
        {Name: "message.delivered"},
        {Name: "message.acked"},
        {Name: "message.dropped"},
    }
    return &pb.LoadedResponse{Hooks: hooks}, nil
}

func (s *HookService) OnProviderUnloaded(ctx context.Context, in *pb.ProviderUnloadedRequest) (*pb.EmptySuccess, error) {
    return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnClientConnect(ctx context.Context, in *pb.ClientConnectRequest) (*pb.EmptySuccess, error) {
    return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnClientConnack(ctx context.Context, in *pb.ClientConnackRequest) (*pb.EmptySuccess, error) {
    return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnClientConnected(ctx context.Context, in *pb.ClientConnectedRequest) (*pb.EmptySuccess, error) {
    log.Debugf("clientInfo %v", in.GetClientinfo())
    ts := time.Now().UnixMilli()
    username := GetUsername(in.Clientinfo)
    ci := &ConnectInfo{
        ClientID:   in.Clientinfo.Clientid,
        UserName:   username,
        PeerHost:   in.Clientinfo.Peerhost,
        Protocol:   in.Clientinfo.Protocol,
        SocketPort: strconv.Itoa(int(in.Clientinfo.Sockport)),
        Online:     true,
        Timestamp:  time.Now().UnixMilli(),
    }
    v, err := EncodeData(*ci)
    if err != nil {
        return nil, err
    }
    infoMap := map[string]interface{}{
        //connectInfoProperty: *ci,
        rawDataProperty: map[string]interface{}{
            "id":     username,
            "ts":     ts,
            "values": v,
            "path":   "",
            "type":   connectInfoProperty,
            "mark":   MarkConnecting,
        },
    }
    // get owner
    owner, err := s.GetState(username + devEntitySuffixKey)
    if err != nil {
        return nil, err
    }
    sw := string(owner)
    // metrics
    s.collector.devTotal.WithLabelValues(sw).Add(1)
    //
    data := map[string]interface{}{
        "id":     username,
        "owner":  sw,
        "type":   "device",
        "source": "iothub",
        "data":   infoMap,
    }
    log.Debugf("iothub->core %s", data)
    if err := s.daprClient.PublishEvent(context.Background(), "iothub-pubsub", s.corePubTopic, data); err != nil {
        log.Error(err)
        return nil, err
    }
    //save ConnectInfo
    ciByte, err := json.Marshal(ci)
    if nil != err {
        log.Error(err)
        return nil, err
    }
    if err := s.SaveState(username+connectInfoSuffixKey, ciByte); err != nil {
        return nil, err
    }

    return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnClientDisconnected(ctx context.Context, in *pb.ClientDisconnectedRequest) (*pb.EmptySuccess, error) {
    username := GetUsername(in.Clientinfo)
    ts := time.Now().UnixMilli()
    ci := &ConnectInfo{
        ClientID:   "",
        UserName:   "",
        PeerHost:   "",
        Protocol:   "",
        SocketPort: "",
        Online:     false,
        Timestamp:  ts,
    }
    v, err := EncodeData(*ci)
    if err != nil {
        return nil, err
    }
    infoMap := map[string]interface{}{
        //connectInfoProperty: *ci,
        rawDataProperty: map[string]interface{}{
            "id":     username,
            "ts":     ts,
            "values": v,
            "path":   "",
            "type":   connectInfoProperty,
            "mark":   MarkConnecting,
        },
    }
    // get owner
    owner, err := s.GetState(username + devEntitySuffixKey)
    // add metrics
    sw := string(owner)
    s.collector.devTotal.WithLabelValues(sw).Add(-1)
    if err != nil {
        return nil, err
    }
    data := map[string]interface{}{
        "id":     username,
        "owner":  sw,
        "type":   "device",
        "source": "iothub",
        "data":   infoMap,
    }
    log.Debugf("iothub->core %s", data)
    if err := s.daprClient.PublishEvent(context.Background(), "iothub-pubsub", s.corePubTopic, data); err != nil {
        log.Error(err)
        return nil, err
    }

    //delete connect info from state store
    if err := s.DeleteState(username + connectInfoSuffixKey); err != nil {
        log.Errorf("Failed to delete state store: %v", err)
        return nil, err
    }

    //delete subId
    if err := s.DeleteState(username + subEntitySuffixKey); nil != err {
        log.Errorf("delete subscription id err, %v", err)
        // get all subscription id
        subIds, err := s.GetState(username)
        if err != nil {
            return nil, err
        }
        // delete topic id and subscription id
        for _, subId := range subIds {
            id := string(subId)
            topic, err := s.GetState(id)
            if err != nil {
                return nil, err
            }
            if err := s.DeleteState(string(topic)); nil != err {
                log.Errorf("delete topic err, %v", err)
                return nil, err
            }
            if err := s.DeleteState(id); nil != err {
                log.Errorf("delete subscription id err, %v", err)
                return nil, err
            }
        }
        // delete device id
        if err := s.DeleteState(username); nil != err {
            log.Errorf("delete device id err, %v", err)
            return nil, err
        }
    }
    return &pb.EmptySuccess{}, nil
}

type TokenValidRequest struct {
    EntityToken string `json:"entity_token"`
}

type TokenValidResponseData struct {
    EntityID   string `json:"entity_id"`
    EntityType string `json:"entity_type"`
    ExpiredAt  string `json:"expired_at"`
    Owner      string `json:"owner"`
    CreatedAt  string `json:"created_at"`
}

type TokenValidResponse struct {
    Code string                 `json:"code"`
    Msg  string                 `json:"msg"`
    Data TokenValidResponseData `json:"data"`
}

func (s *HookService) parseToken(password string) (*TokenValidResponse, error) {
    url := BaseUrl + "/security/v1/entity/info/" + password
    req, err := http.NewRequest(http.MethodGet, url, nil)
    if err != nil {
        return nil, err
    }

    req.Header.Add("Content-Type", "application/json")
    AddDefaultAuthHeader(req)

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return nil, err
    }

    defer resp.Body.Close()

    if resp.StatusCode != 200 {
        return nil, errors.New("Invalid StatusCode " + resp.Status)
    }

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }

    tokenResp := &TokenValidResponse{}
    if err := json.Unmarshal(body, tokenResp); nil != err {
        return nil, err
    }
    return tokenResp, nil
}

func (s *HookService) auth(password, username string) bool {
    tokenResp, err := s.parseToken(password)
    if nil != err {
        log.Error(err)
        return false
    }
    log.Debug(tokenResp, username)
    if tokenResp.Data.EntityID != username {
        log.Errorf("invalid username %s", username)
        return false
    }
    //save owner
    if err := s.SaveState(username+devEntitySuffixKey, []byte(tokenResp.Data.Owner)); err != nil {
        return false
    }
    return true
}

func GetUsername(Clientinfo *pb.ClientInfo) string {
    // coap 协议 用户名最大支持5个字符
    protocol := Clientinfo.GetProtocol()
    var username string
    if protocol == "coap" {
        username = Clientinfo.GetClientid()
    } else if protocol == "lwm2m" {
        username = SplitLwm2mClientID(Clientinfo.GetClientid(), 0)
    } else {
        username = Clientinfo.GetUsername()
    }
    return username
}

func SplitLwm2mClientID(lwm2mClientID string, index int) string {
    // LwM2M client id should be username@password
    idArray := strings.Split(lwm2mClientID, "@")
    if len(idArray) < 2 || index > (len(idArray)+1) {
        return ""
    }
    return idArray[index]
}

func GetPassword(Clientinfo *pb.ClientInfo) string {
    protocol := Clientinfo.GetProtocol()
    var pw string
    if protocol == "lwm2m" {
        pw = SplitLwm2mClientID(Clientinfo.GetClientid(), 1)
    } else {
        pw = Clientinfo.GetPassword()
    }
    return pw
}

func (s *HookService) OnClientAuthenticate(ctx context.Context, in *pb.ClientAuthenticateRequest) (*pb.ValuedResponse, error) {
    res := &pb.ValuedResponse{}
    res.Type = pb.ValuedResponse_STOP_AND_RETURN
    log.Debug(in.GetClientinfo())
    username := GetUsername(in.Clientinfo)
    pw := GetPassword(in.Clientinfo)
    if username == "" || pw == "" {
        log.Warnf("invalid username %s or password %s", username, pw)
        return res, nil
    }
    authRes := s.auth(pw, username)
    res.Value = &pb.ValuedResponse_BoolResult{BoolResult: authRes}
    return res, nil
}

func (s *HookService) OnClientCheckAcl(ctx context.Context, in *pb.ClientCheckAclRequest) (*pb.ValuedResponse, error) { //nolint
    return &pb.ValuedResponse{}, nil
}

func (s *HookService) OnClientSubscribe(ctx context.Context, in *pb.ClientSubscribeRequest) (*pb.EmptySuccess, error) {
    topics := in.GetTopicFilters()
    username := in.Clientinfo.GetUsername()
    for _, tf := range topics {
        topic := tf.GetName()
        // 获取设备token(mqtt 的 pwd)
        value, err := s.GetState(username + devEntitySuffixKey)
        if err != nil {
            return nil, err
        }
        owner := string(value)

        if !validSubTopic(topic) {
            log.Errorf("invalid topic:%s username:%s", topic, username)
            return nil, errors.New("invalid topic")
        }

        itemType := "*"
        log.Debugf("client subscribe:%s itemType:%", owner, itemType)
        // TODO: 优化逻辑
        if err := s.CreateSubscribeEntity(owner, username, itemType, topic, realtimeMode); err != nil {
            return nil, err
        }
    }
    return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnClientUnsubscribe(ctx context.Context, in *pb.ClientUnsubscribeRequest) (*pb.EmptySuccess, error) {
    var (
        err error
    )
    // TODO: 1. 删除对应的 topic
    topics := in.GetTopicFilters()
    // tips: username == devId
    username := in.Clientinfo.GetUsername()
    for _, tf := range topics {
        topic := tf.GetName()
        topic = buildTopic(username, topic)
        log.Debug("unSubscribe topic ", topic)
        if e := s.DeleteState(topic); nil != e {
            log.Errorf("delete subscription id err, %v", e)
            if err != nil {
                err = e
            }
        }
    }
    // TODO: 2. 如果所有的 topic 都取消完了则删除设备在 core 里面的订阅
    return &pb.EmptySuccess{}, err
}

func (s *HookService) OnSessionCreated(ctx context.Context, in *pb.SessionCreatedRequest) (*pb.EmptySuccess, error) {
    return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnSessionSubscribed(ctx context.Context, in *pb.SessionSubscribedRequest) (*pb.EmptySuccess, error) {
    return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnSessionUnsubscribed(ctx context.Context, in *pb.SessionUnsubscribedRequest) (*pb.EmptySuccess, error) {
    return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnSessionResumed(ctx context.Context, in *pb.SessionResumedRequest) (*pb.EmptySuccess, error) {
    return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnSessionDiscarded(ctx context.Context, in *pb.SessionDiscardedRequest) (*pb.EmptySuccess, error) {
    return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnSessionTakeovered(ctx context.Context, in *pb.SessionTakeoveredRequest) (*pb.EmptySuccess, error) {
    return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnSessionTerminated(ctx context.Context, in *pb.SessionTerminatedRequest) (*pb.EmptySuccess, error) {
    return &pb.EmptySuccess{}, nil
}

// encode data
func EncodeData(jsonData interface{}) ([]byte, error) {
    byteData, err := json.Marshal(jsonData)
    if nil != err {
        return nil, err
    }
    return byteData, nil
}

func getUserNameFromTopic(topic string) (user string) {
    items := strings.SplitN(topic, "/", 2)
    if len(items) != 2 {
        return
    }
    // lwm2m protocol
    if items[0] == "lwm2m" {
        lwm2mClientId := getUserNameFromTopic(items[1])
        username := SplitLwm2mClientID(lwm2mClientId, 0)
        return username
    }
    return items[0]
}

type Event struct {
    cloudevents.Event
}

func toCloudEventData(data interface{}) ([]byte, error) {
    m := make(map[string]interface{})
    m["traceid"] = uuid.New().String()
    m["id"] = uuid.New().String()
    m["topic"] = "core-pub"
    m["pubsubname"] = "iothub-pubsub"
    m["source"] = "iothub"
    m["type"] = "com.dapr.event.sent"
    m["specversion"] = "1.0"
    m["data"] = data
    //
    return jsoniter.Marshal(m)
}

func topicFromUserNameTopic(userNameTopic string) string {
    vv := strings.SplitN(userNameTopic, "/", 2)
    if len(vv) > 0 {
        return vv[1]
    }
    return ""
}

func (s *HookService) OnMessagePublish(ctx context.Context, in *pb.MessagePublishRequest) (*pb.ValuedResponse, error) {
    res := &pb.ValuedResponse{}
    res.Type = pb.ValuedResponse_STOP_AND_RETURN
    res.Value = &pb.ValuedResponse_BoolResult{BoolResult: false}
    username := getUserNameFromTopic(in.Message.Topic)
    //get owner
    owner, err := s.GetState(username + devEntitySuffixKey)
    log.Infof("find username: %s owner: %s", username, owner)
    if err != nil {
        return nil, err
    }
    sw := string(owner)
    //do nothing when receive tkeel attribute/telemetry/command event.
    // 下行数据直接返回
    // add metrics
    if in.Message.From == defaultDownStreamClientId {
        s.collector.msgTotal.WithLabelValues(sw, MarkDownStream).Add(1)
        log.Debugf("downstream data: %v", in.GetMessage())
        res.Value = &pb.ValuedResponse_BoolResult{BoolResult: true}
        return res, nil
    }
    //
    s.collector.msgTotal.WithLabelValues(sw, MarkUpStream).Add(1)
    //
    data := make(map[string]interface{})
    data["id"] = username
    data["owner"] = string(owner)
    data["type"] = "device"
    data["source"] = "iothub"
    // username = deviceId
    // 此处topic为 user/topic
    userNameTopic := in.GetMessage().Topic

    payloadBytes := in.GetMessage().GetPayload()
    log.Infof("receive topic: %s payload: %s", userNameTopic, string(payloadBytes))

    /*
    	{
    	   "id": "device_123",
    	   "ts": 1641349927430079500,
    	   "path": "device_123/v1/devices/me/telemetry"
    	   "values": {
    	        "telemetry1": "value1",
    	        "telemetry2": "value2"
    	   },
    	   "type": "telemetry",
    	   "mark": "upstream"
    	}
    */

    topic := topicFromUserNameTopic(userNameTopic)
    if topic == "" {
        res.Value = &pb.ValuedResponse_BoolResult{BoolResult: true}
    }

    propertyType := propertyTypeFromTopic(topic)
    // TODO: propertyType check
    ts := time.Now().UnixMilli()
    md := map[string]interface{}{
        rawDataProperty: map[string]interface{}{
            "id":     username,
            "ts":     ts,
            "values": payloadBytes,
            "path":   userNameTopic,
            "type":   propertyType,
            "mark":   MarkUpStream,
        },
    }
    data["data"] = md
    dd, err := toCloudEventData(data)
    if err != nil {
        log.Errorf("toCloudEventData %s", err.Error())
        return res, err
    }
    s.producer.Input() <- &sarama.ProducerMessage{
        Topic: s.corePubTopic,
        Value: sarama.ByteEncoder(dd),
        Key:   sarama.StringEncoder(username),
    }
    log.Debug("OnMessagePublish", data)
    //if err := s.daprClient.PublishEvent(context.Background(), "iothub-pubsub", s.corePubTopic, data); err != nil {
    //	log.Error(err)
    //	return res, nil
    //}
    res.Value = &pb.ValuedResponse_BoolResult{BoolResult: true}
    return res, nil
}

func (s *HookService) OnMessageDelivered(ctx context.Context, in *pb.MessageDeliveredRequest) (*pb.EmptySuccess, error) {
    return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnMessageDropped(ctx context.Context, in *pb.MessageDroppedRequest) (*pb.EmptySuccess, error) {
    return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnMessageAcked(ctx context.Context, in *pb.MessageAckedRequest) (*pb.EmptySuccess, error) {
    return &pb.EmptySuccess{}, nil
}

func AddDefaultAuthHeader(req *http.Request) {
    authString := fmt.Sprintf("tenant=%s&user=%s&role=%s", defaultTenant, defaultUser, defultRole)
    req.Header.Add(tkeelAuthHeader, base64.StdEncoding.EncodeToString([]byte(authString)))
}

// create SubscribeEntity
func (s *HookService) CreateSubscribeEntity(owner, devId, itemType, subscriptionTopic, subscriptionMode string) error {
    // md5(devId+itemType)
    // IoTHub 订阅 “*”
    //
    if itemType != "*" {
        itemType = "*"
    }
    subId := fmt.Sprintf("sub-%x", md5.Sum([]byte(devId+itemType)))
    //subId := fmt.Sprintf("%s%s", "sub-", GetUUID())
    subReq := &v1.SubscriptionObject{
        PubsubName: "iothub-pubsub",
        Topic:      "sub-core",
        Mode:       subscriptionMode,
        Filter:     fmt.Sprintf("insert into %s select %s.%s", subId, devId, itemType),
        Source:     "tkeel-device",
        Target:     "iothub",
    }
    log.Debug("create SubscribeEntity: ", subReq)

    data, err := json.Marshal(subReq)
    if nil != err {
        log.Error(err)
        return err
    }

    url := fmt.Sprintf(BaseUrl+"/core/v1/subscriptions?id=%s&source=%s&owner=%s&type=%s", subId, "iothub", owner, "SUBSCRIPTION")
    payload := strings.NewReader(string(data))
    req, err := http.NewRequest(http.MethodPost, url, payload)
    if err != nil {
        return err
    }
    req.Header.Add("Content-Type", "application/json")
    AddDefaultAuthHeader(req)

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    res, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        log.Errorf("create subscription err, %v", err)
        return err
    }

    // TODO: 保存订阅 subid 一个设备只会有一个 subId 然后由 iothub 区分同的 topic
    if err := s.SaveState(subId, []byte(devId)); err != nil {
        return err
    }

    // TODO: 关联deviceId 与 subId
    if err := s.SaveState(devId, []byte(subId)); err != nil {
        return err
    }
    // TODO: 保存订阅的 topic = deviceId/subscriptionTopic 取消订阅即是删除对应的 topic
    // TODO: 保存设备订阅的 topic 一个设备可能会订阅多个 topic 所以这里使用 deviceId + topic 作为唯一key

    tp := buildTopic(devId, subscriptionTopic)
    log.Debugf("create subscription ok, %v", res)
    if err := s.SaveState(tp, []byte("T")); err != nil {
        return err
    }

    return nil
}

// 保存每一个设备的 subIds
func (s *HookService) SaveSubscriptionId(devId, subId string) error {
    subscriptionIds, err := s.GetState(devId)
    if err != nil {
        return err
    }
    var subIds interface{}

    if err := json.Unmarshal(subscriptionIds, &subIds); nil != err {
        return err
    }
    idArray := reflect.ValueOf(subIds)
    idArray.Field(idArray.Len()).SetString(subId)
    newSubIds, err := json.Marshal(idArray.Interface())
    if err != nil {
        return err
    }
    if err := s.SaveState(devId, newSubIds); err != nil {
        return err
    }
    return nil
}

// delete SubscribeEntity
func (s *HookService) DeleteSubscribeEntity(owner, devId, subId string) error {
    url := fmt.Sprintf("apis/core/v1/subscriptions/%s?source=%s&owner=%s&type=%s", subId, "iothub", owner, "SUBSCRIPTION")
    res, err := s.daprClient.InvokeMethodWithContent(
        context.Background(),
        "keel",
        url,
        http.MethodDelete,
        &dapr.DataContent{
            ContentType: "application/json",
        },
    )
    if err != nil {
        log.Errorf("delete subscription entity err %v", err)
        return err
    }
    log.Debugf("delete subscription ok, %v", res)
    return nil
}

// get state store
func (s *HookService) GetState(key string) ([]byte, error) {
    item, err := s.daprClient.GetState(context.Background(), iothubPrivateStatesStoreName, key)
    if err != nil {
        log.Errorf("Failed to get state: %v", err)
        return nil, err
    }
    return item.Value, nil
}

// save state store
func (s *HookService) SaveState(key string, data []byte) error {
    if err := s.daprClient.SaveState(context.Background(), iothubPrivateStatesStoreName, key, data); err != nil {
        log.Errorf("Failed to persist state: %v\n", err)
        return err
    }
    return nil
}

// delete state store
func (s *HookService) DeleteState(key string) error {
    if err := s.daprClient.DeleteState(context.Background(), iothubPrivateStatesStoreName, key); err != nil {
        log.Errorf("Failed to delete state store: %v", err)
        return err
    }
    return nil
}
