package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	pb "github.com/tkeel-io/iothub/api/iothub/v1"
	"github.com/tkeel-io/kit/log"
)

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

func (s *TopicService) TopicEventHandler(ctx context.Context, req *pb.TopicEventRequest) (out *pb.TopicEventResponse, err error) {
	log.Debugf("receive pubsub topic: %s, payload: %v", req.GetTopic(), req.GetData())
	// get subId
	id := req.GetId()
	var payload interface{}

	switch kv := req.Data.AsInterface().(type) {
	case map[string]interface{}:
		switch kvv := kv["properties"].(type) {
		case map[string]interface{}:
			for k, v := range kvv {
				log.Debugf("k: %s, v: %v", k, v)
				if k == telemetryProperty || k == attributeProperty || k == commandProperty {
					data := convertBase64ToMap(v.(string))
					log.Debugf("k: %s, data: %v", k, data)
					topic := data["topic"].(string)
					username := getUserNameFromTopic(topic)
					subId, err := s.hookSvc.GetState(username, subEntitySuffixKey)
					if nil != err {
						continue
					}
					payload = data["data"]
					log.Infof("id: %s, subId: %s, payload: %v", id, subId, payload)
					// publish(post) data to emq
					if err := Publish(username, topic, defaultDownStreamClientId, 0, false, payload); nil != err {
						return &pb.TopicEventResponse{Status: SubscriptionResponseStatusDrop}, err
					}
				} else {
					log.Debugf("unused k: %v, v: %v", k, v)
					return &pb.TopicEventResponse{Status: SubscriptionResponseStatusDrop}, nil
				}
			}

		default:
			return &pb.TopicEventResponse{Status: SubscriptionResponseStatusDrop}, nil
		}

	default:
		return &pb.TopicEventResponse{Status: SubscriptionResponseStatusDrop}, nil
	}
	return &pb.TopicEventResponse{Status: SubscriptionResponseStatusDrop}, nil
}

func convertBase64ToMap(base64String string) map[string]interface{} {
	var mapData map[string]interface{}

	sDec, _ := base64.StdEncoding.DecodeString(base64String)

	if err := json.Unmarshal(sDec, &mapData); nil != err {
		log.Errorf("unmarshal err %s", err)
		return nil
	}
	return mapData
}
