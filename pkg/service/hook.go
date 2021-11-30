package service

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	dapr "github.com/dapr/go-sdk/client"

	pb "github.com/tkeel-io/iothub/protobuf"
	"github.com/tkeel-io/kit/log"
)

// HookService is used to implement emqx_exhook_v1.s *HookService.
type HookService struct {
	pb.UnimplementedHookProviderServer
	daprClient dapr.Client
}

func NewHookService(client dapr.Client) *HookService {
	return &HookService{daprClient: client}
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
	return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnClientDisconnected(ctx context.Context, in *pb.ClientDisconnectedRequest) (*pb.EmptySuccess, error) {
	return &pb.EmptySuccess{}, nil
}

type TokenValidRequest struct {
	EntityToken string `json:"entity_token"`
}

type TokenValidResponseData struct {
	EntityID   string `json:"entity_id"`
	EntityType string `json:"entity_type"`
	Exp        int64  `json:"exp"`
	Owner      string `json:"owner"`
}

type TokenValidResponse struct {
	Code int32                  `json:"code"`
	Msg  string                 `json:"msg"`
	Data TokenValidResponseData `json:"data"`
}

func (s *HookService) auth(username, password, clientid string) bool {
	tokenReq := TokenValidRequest{EntityToken: password}
	resp, err := s.daprClient.InvokeMethodWithCustomContent(context.Background(), "keel", "apis/security/v1/entity/token/valid", http.MethodPost, "application/json", tokenReq)
	if err != nil {
		log.Error(err)
		return false
	}

	tokenResp := &TokenValidResponse{}
	json.Unmarshal(resp, tokenResp)
	return tokenResp.Data.EntityID == clientid && tokenResp.Data.Owner == username
}

func (s *HookService) OnClientAuthenticate(ctx context.Context, in *pb.ClientAuthenticateRequest) (*pb.ValuedResponse, error) {
	res := &pb.ValuedResponse{}
	res.Type = pb.ValuedResponse_STOP_AND_RETURN
	log.Debug(in.GetClientinfo())
	authRes := s.auth(in.Clientinfo.GetUsername(), in.Clientinfo.GetPassword(), in.Clientinfo.GetClientid())
	res.Value = &pb.ValuedResponse_BoolResult{BoolResult: authRes}
	return res, nil
}

func (s *HookService) OnClientCheckAcl(ctx context.Context, in *pb.ClientCheckAclRequest) (*pb.ValuedResponse, error) { //nolint
	return &pb.ValuedResponse{}, nil
}

func (s *HookService) OnClientSubscribe(ctx context.Context, in *pb.ClientSubscribeRequest) (*pb.EmptySuccess, error) {
	return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnClientUnsubscribe(ctx context.Context, in *pb.ClientUnsubscribeRequest) (*pb.EmptySuccess, error) {
	return &pb.EmptySuccess{}, nil
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

func getUserNameFromTopic(topic string) (user string) {
	items := strings.SplitN(topic, "/", 2)
	if len(items) != 2 {
		return
	}
	user = items[0]
	return
}

func (s *HookService) OnMessagePublish(ctx context.Context, in *pb.MessagePublishRequest) (*pb.ValuedResponse, error) {
	entityID := in.GetMessage().GetFrom()
	userName := getUserNameFromTopic(in.GetMessage().GetTopic())

	publishData := make(map[string]interface{})
	publishData["id"] = entityID
	publishData["owner"] = userName

	data := make(map[string]interface{})
	dataRaw := in.GetMessage().GetPayload()
	if err := json.Unmarshal(dataRaw, &data); err != nil {
		data["_data_"] = dataRaw
	}
	publishData["data"] = data
	log.Debug(publishData)
	if err := s.daprClient.PublishEvent(context.Background(), "iothub-pubsub", "core-pub", publishData); err != nil {
		log.Error(err)
	}

	return &pb.ValuedResponse{}, nil
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
