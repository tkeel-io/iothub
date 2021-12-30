package service

import (
	"context"
	pb "github.com/tkeel-io/iothub/api/iothub/v1"
	"github.com/tkeel-io/kit/log"
)

type TopicService struct {
	pb.UnimplementedTopicServer
	ctx           context.Context
	cancel        context.CancelFunc
	hookSvc       *HookService
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
		ctx:           ctx,
		cancel:        cancel,
		hookSvc: 	   hookSvc,
	}, nil
}

func (s *TopicService) TopicEventHandler(ctx context.Context, req *pb.TopicEventRequest) (out *pb.TopicEventResponse, err error) {
	log.Debugf("receive pubsub topic: %s, payload: %v", req.GetTopic(), req.GetData())
	username := getUserNameFromTopic(req.Topic)
	topic := req.Topic[len(username):]
	payload := req.Data

	// get subId
	id := req.GetId()
	subId, err := s.hookSvc.GetState(username, subEntitySuffixKey)
	if nil != err{
		return &pb.TopicEventResponse{Status: SubscriptionResponseStatusDrop}, err
	}
	// publish(post) data to emq
	if id == string(subId) {
		if err := Publish(username, topic, defaultDownStreamClientId, 0,false, payload); nil != err{
			return &pb.TopicEventResponse{Status: SubscriptionResponseStatusDrop}, err
		}
	}
	//delete subId
	if err:= s.hookSvc.DeleteState(username, subEntitySuffixKey); nil != err {
		log.Errorf("delete subscription id err, %v", err)
		return &pb.TopicEventResponse{Status: SubscriptionResponseStatusDrop}, err
	}
	return &pb.TopicEventResponse{Status: SubscriptionResponseStatusSuccess}, nil
}
