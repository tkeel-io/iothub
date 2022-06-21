package service

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	pb "github.com/tkeel-io/iothub/api/iothub/v1"
	"github.com/tkeel-io/kit/log"

	"github.com/tidwall/gjson"
)

var _validTopics = map[string]string{
	DeviceDebugTopic: "-",
	AttributesTopic:  _attrPropPath,
	CommandTopic:     _cmdPropPath,
	RawDataTopic:     _rawPropPath,
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

	producer sarama.AsyncProducer
	// the topic pub to core
	corePubTopic string
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

	ctx, cancel := context.WithCancel(ctx)
	return &TopicService{
		ctx:          ctx,
		cancel:       cancel,
		hookSvc:      hookSvc,
		producer:     p,
		corePubTopic: envWithDefault(_envCorePubTopic, "core-pub"),
	}, nil
}

func getValue(strReqJson string, subTypePath string) (bool, interface{}) {
	res := gjson.Get(strReqJson, subTypePath)
	//
	return res.Exists(), res.Value()
}

//
func buildTopic(devId, topic string) string {
	return strings.Join([]string{devId, topic}, "/")
}

//
const (
	_cmdPropPath  = `properties.commands`
	_attrPropPath = `properties.attributes`
	// 原始数据只取 value 后面的
	_rawPropPath = `properties.rawDown.value`
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
	var dataValue interface{}
	userNameTopic := ""
	if propPath, ok := getKeyFromTopic(topic); ok && propPath != "" {
		ok, dataValue = getValue(strReqJson, propPath)
		if !ok || dataValue == nil {
			return &pb.TopicEventResponse{Status: SubscriptionResponseStatusDrop}, err
		}

		userNameTopic = buildTopic(devId, topic)

		if err = Publish(devId, userNameTopic, defaultDownStreamClientId, 0, false, dataValue); err != nil {
			log.Errorf("TopicEventHandler: topic=%s err=%v", userNameTopic, err)
			return &pb.TopicEventResponse{Status: SubscriptionResponseStatusSuccess}, err
		}
	}

	payloadBytes, err := json.Marshal(dataValue)
	if err != nil {
		return &pb.TopicEventResponse{Status: SubscriptionResponseStatusDrop}, err
	}
	owner := gjson.Get(strReqJson, "owner").String()
	data := make(map[string]interface{})
	data["id"] = devId
	data["owner"] = owner
	data["type"] = "device"
	data["source"] = "iothub"
	ts := time.Now().UnixMilli()
	md := map[string]interface{}{
		rawDataProperty: map[string]interface{}{
			"id":     devId,
			"ts":     ts,
			"values": payloadBytes,
			"path":   userNameTopic,
			"type":   rawDataProperty,
			"mark":   MarkDownStream,
		},
	}
	data["data"] = md
	dd, err := toCloudEventData(data)
	if err != nil {
		log.Errorf("toCloudEventData %s", err.Error())
		return &pb.TopicEventResponse{Status: SubscriptionResponseStatusDrop}, err
	}
	s.producer.Input() <- &sarama.ProducerMessage{
		Topic: s.corePubTopic,
		Value: sarama.ByteEncoder(dd),
		Key:   sarama.StringEncoder(devId),
	}
	log.Debug("OnMessagePublish", data)

	return &pb.TopicEventResponse{Status: SubscriptionResponseStatusDrop}, err
}
