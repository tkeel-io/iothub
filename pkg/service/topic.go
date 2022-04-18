package service

import (
    "context"
    pb "github.com/tkeel-io/iothub/api/iothub/v1"
    "github.com/tkeel-io/kit/log"
    "strings"

    "github.com/tidwall/gjson"
)

var _validSubTopics = map[string]string{
    DeviceDebugTopic:       "-",
    AttributesTopic:        "attributes",
    CommandTopic:           "commands",
    AttributesGatewayTopic: "attributes",
    RawDataTopic:           "raw",
}

func validSubTopic(topic string) bool {
    if _, ok := _validSubTopics[topic]; ok {
        return true
    }
    return false
}

func getKeyFromTopic(topic string) (string, bool) {
    if v, ok := _validSubTopics[topic]; ok {
        return v, ok
    }
    return "", false
}

func subscribeTopics() []string {
    return []string{AttributesTopic, AttributesGatewayTopic, RawDataTopic, CommandTopic}
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

func getValue(strReqJson, subType string) (bool, interface{}) {
    jsonPath := strings.Join([]string{"properties", subType}, ".")
    res := gjson.Get(strReqJson, jsonPath)
    //
    return res.IsObject(), res.Value()
}

//
func buildTopic(devId, topic string, ) string {
    return strings.Join([]string{devId, topic}, "/")
}

func (s *TopicService) TopicEventHandler(ctx context.Context, req *pb.TopicEventRequest) (out *pb.TopicEventResponse, err error) {
    log.Debugf("receive pubsub topic: %s, payload: %v", req.GetTopic(), req.GetData())
    //get subId
    bys, err := req.Data.MarshalJSON()
    if err != nil {
        return nil, err
    }
    // TODO: 根据变化的内容确定发送到哪个 topic
    strReqJson := string(bys)
    devId := gjson.Get(strReqJson, "id").String()
    // 合法的订阅 topic
    topics := subscribeTopics()
    for _, tp := range topics {
        respTp := buildTopic(devId, tp)
        if bys, err := s.hookSvc.GetState(respTp); err != nil || len(bys) == 0 {
            log.Errorf("TopicEventHandler: topic=%s", respTp)
            continue
        }
        // 根据从 core 来的消息处理不同的 topic
        if ekey, ok := getKeyFromTopic(tp); ok && ekey != "" {
            ok, v := getValue(strReqJson, ekey)
            if !ok && v == nil {
                continue
            }
            if err = Publish(devId, respTp, defaultDownStreamClientId, 0, false, v); err != nil {
                log.Errorf("TopicEventHandler: topic=%s err=%v", respTp, err)
            }
        }
    }
    if err != nil {
        return &pb.TopicEventResponse{Status: SubscriptionResponseStatusDrop}, err
    }
    return &pb.TopicEventResponse{Status: SubscriptionResponseStatusSuccess}, err
}
