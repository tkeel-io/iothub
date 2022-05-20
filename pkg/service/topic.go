package service

import (
    "context"
    "encoding/json"
    pb "github.com/tkeel-io/iothub/api/iothub/v1"
    "github.com/tkeel-io/kit/log"
    "strings"

    "github.com/tidwall/gjson"
)

var _validTopics = map[string]string{
    DeviceDebugTopic:       "-",
    AttributesTopic:        _attrPropPath,
    CommandTopic:           _cmdPropPath,
    RawDataTopic:           _rawPropPath,
}

func validSubTopic(topic string) bool {
    if _, ok := _validTopics[topic]; ok {
        return true
    }
    return false
}

func getKeyFromTopic(topic string) (string, bool) {
    if v, ok := _validTopics[topic]; ok {
        return v, ok
    }
    return "", false
}

type TopicService struct {
    pb.UnimplementedTopicServer
    ctx     context.Context
    cancel  context.CancelFunc
    hookSvc *HookService
}

const (
    // SubscriptionResponseStatusSuccess means message is processed successfully.
    SubscriptionResponseStatusSuccess = "SUCCESS"
    // SubscriptionResponseStatusRetry means message to be retried by Dapr.
    SubscriptionResponseStatusRetry = "RETRY"
    // SubscriptionResponseStatusDrop means warning is logged and message is dropped.
    SubscriptionResponseStatusDrop = "DROP"
)

func NewTopicService(ctx context.Context, hookSvc *HookService) (*TopicService, error) {
    ctx, cancel := context.WithCancel(ctx)

    return &TopicService{
        ctx:     ctx,
        cancel:  cancel,
        hookSvc: hookSvc,
    }, nil
}

func getValue(strReqJson string, subTypePath string) (bool, interface{}) {
    res := gjson.Get(strReqJson, subTypePath)
    //
    return res.Exists(), res.Value()
}

//
func buildTopic(devId, topic string, ) string {
    return strings.Join([]string{devId, topic}, "/")
}

//
const (
    _cmdPropPath  = `properties.commands`
    _attrPropPath = `properties.attributes`
    _rawPropPath  = `properties.rawDown`
)

/**
const DeviceDebugTopic = "v1/devices/debug"
const RawDataTopic string = "v1/devices/me/raw"
const CommandTopic string = "v1/devices/me/commands"
const AttributesTopic string = "v1/devices/me/attributes"
const AttributesGatewayTopic = "v1/gateway/attributes"
*/
func getTopicFromCoreReq(strReqJson string) string {
    if ok, v := getValue(strReqJson, _cmdPropPath); ok {
        b, err := json.Marshal(v)
        if err != nil {
            return ""
        }
        s := string(b)
        if strings.Contains(s, "input") {
            return CommandTopic
        }
        if strings.Contains(s, "output") {
            return CommandTopicResponse
        }
        return ""
    }
    //
    if ok, _ := getValue(strReqJson, _attrPropPath); ok {
        return AttributesTopic
    }
    //
    if ok, _ := getValue(strReqJson, _rawPropPath); ok {
        return RawDataTopic
    }
    //
    return ""
}

//
func (s *TopicService) TopicEventHandler(ctx context.Context, req *pb.TopicEventRequest) (out *pb.TopicEventResponse, err error) {
    log.Debugf("receive pubsub topic: %s, payload: %v", req.GetTopic(), req.GetData())

    bys, err := req.Data.MarshalJSON()
    if err != nil {
        return nil, err
    }
    log.Debugf("receive pubsub topic: %s, payload: %v", req.GetTopic(), string(bys))
    // TODO: 根据变化的内容确定发送到哪个 topic
    strReqJson := string(bys)
    devId := gjson.Get(strReqJson, "id").String()

    // TODO: topic 验证
    topic := getTopicFromCoreReq(strReqJson)

    if !validSubTopic(topic) {
        return &pb.TopicEventResponse{Status: SubscriptionResponseStatusSuccess}, err
    }

    // 根据从 core 来的消息处理不同的 topic
    if propPath, ok := getKeyFromTopic(topic); ok && propPath != "" {
        ok, v := getValue(strReqJson, propPath)
        if !ok || v == nil {
            return &pb.TopicEventResponse{Status: SubscriptionResponseStatusDrop}, err
        }
        userNameTopic := buildTopic(devId, topic)
        if err = Publish(devId, userNameTopic, defaultDownStreamClientId, 0, false, v); err != nil {
            log.Errorf("TopicEventHandler: topic=%s err=%v", userNameTopic, err)
            return &pb.TopicEventResponse{Status: SubscriptionResponseStatusSuccess}, err
        }
    }

    return &pb.TopicEventResponse{Status: SubscriptionResponseStatusDrop}, err
}
