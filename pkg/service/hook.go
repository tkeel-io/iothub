package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	dapr "github.com/dapr/go-sdk/client"

	pb "github.com/tkeel-io/iothub/protobuf"
	"github.com/tkeel-io/kit/log"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	v1 "github.com/tkeel-io/core/api/core/v1"
)

// HookService is used to implement emqx_exhook_v1.s *HookService.
type HookService struct {
	pb.UnimplementedHookProviderServer
	daprClient dapr.Client
	clients map[string]*ConnectInfo
	entities map[string]*EntityInfo
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
	log.Debugf("clientInfo %v", in.GetClientinfo())

	node := in.Clientinfo.Node
	if s.clients != nil {
		if _, ok := s.clients[node]; !ok{
			log.Warn("repeated Node", node)
			return &pb.EmptySuccess{}, nil
		}
	} else {
		s.clients = make(map[string]*ConnectInfo)
	}

	ci := &ConnectInfo{
		ClientID:   in.Clientinfo.Clientid,
		UserName:   in.Clientinfo.Username,
		PeerHost:   in.Clientinfo.Peerhost,
		Protocol:   in.Clientinfo.Protocol,
		SocketPort: strconv.Itoa(int(in.Clientinfo.Sockport)),
	}
	s.clients[node] = ci
	infoMap :=map[string]interface{}{
		"connectInfo": *ci,
	}
	// get device entity info
	entity := s.GetEntityInfo(in.GetClientinfo().Node)
	if nil == entity {
		log.Warn("unknown message", in.GetClientinfo())
		return &pb.EmptySuccess{}, nil
	}
	data := map[string]interface{}{
		"id": entity.EntityID,
		"owner": entity.Owner,
		"type": "device",
		"source": "iothub",
		"data": infoMap,
	}
	log.Debugf("pub data %s", data)
	if err := s.daprClient.PublishEvent(context.Background(), "iothub-pubsub", "core-pub", data); err != nil {
		log.Error(err)
	}
	return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnClientDisconnected(ctx context.Context, in *pb.ClientDisconnectedRequest) (*pb.EmptySuccess, error) {
	node := in.Clientinfo.Node
	if s.clients != nil {
		if _, ok := s.clients[node]; !ok{
			log.Warn("repeated Node", node)
		}
	} else {
		s.clients = make(map[string]*ConnectInfo)
	}
	ci := &ConnectInfo{
		ClientID: "",
		UserName: "",
		PeerHost: "",
		Protocol: "",
		SocketPort: "",
	}
	s.clients[node] = ci
	infoMap :=map[string]interface{}{
		"connectInfo": *ci,
	}
	// get device entity info
	entity := s.GetEntityInfo(in.GetClientinfo().Node)
	if nil == entity {
		log.Warn("unknown message", in.GetClientinfo())
		return &pb.EmptySuccess{}, nil
	}
	data := map[string]interface{}{
		"id": entity.EntityID,
		"owner": entity.Owner,
		"type": "device",
		"source": "iothub",
		"data": infoMap,
	}
	log.Debugf("pub data %s", data)
	if err := s.daprClient.PublishEvent(context.Background(), "iothub-pubsub", "core-pub", data); err != nil {
		log.Error(err)
	}
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

func (s *HookService) parseToken(password string) (*TokenValidResponse, error) {
	url :=  "apis/security/v1/entity/info/" + password
	resp, err := s.daprClient.InvokeMethod(context.Background(), "keel", url, http.MethodGet)
	if err != nil {
		return nil, err
	}

	tokenResp := &TokenValidResponse{}
	if err := json.Unmarshal(resp, tokenResp); nil != err{
		return nil, err
	}
	return tokenResp, nil
}

func (s *HookService) auth(password, node  string) bool {
	tokenResp, err := s.parseToken(password)
	if nil != err{
		log.Error(err)
		return false
	}
	if tokenResp.Code != 0 {
		log.Warn(tokenResp.Msg)
		return false
	}
	if s.entities == nil {
		s.entities = make(map[string]*EntityInfo)
	}
	s.entities[node] = &EntityInfo{
		EntityID: tokenResp.Data.EntityID,
		Owner: tokenResp.Data.Owner,
	}
	return true
}

func (s *HookService) OnClientAuthenticate(ctx context.Context, in *pb.ClientAuthenticateRequest) (*pb.ValuedResponse, error) {
	res := &pb.ValuedResponse{}
	res.Type = pb.ValuedResponse_STOP_AND_RETURN
	log.Debug(in.GetClientinfo())
	authRes := s.auth(in.Clientinfo.GetPassword(), in.Clientinfo.GetNode())
	res.Value = &pb.ValuedResponse_BoolResult{BoolResult: authRes}
	return res, nil
}

func (s *HookService) OnClientCheckAcl(ctx context.Context, in *pb.ClientCheckAclRequest) (*pb.ValuedResponse, error) { //nolint
	return &pb.ValuedResponse{}, nil
}

func (s *HookService) OnClientSubscribe(ctx context.Context, in *pb.ClientSubscribeRequest) (*pb.EmptySuccess, error) {
	//in.GetClientinfo().
	topics := in.GetTopicFilters()
	for _, tf := range topics{
		topic := tf.GetName()
		if topic == AttributesTopic {
			//todo create pubsub and subscription
			//CreateSubscription()

		}else if topic == TelemetryTopic {
			//todo create pubsub and subscription

		}else if  topic == (CommandTopicRequest + "+"){
			//订阅平台命令
			//do nothing
		}else if  topic == (AttributesTopicResponse + "+"){
			//订阅平台属性
			//do nothing
		}else{
			return nil, errors.New("invalid topic")
		}

	}
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

// get time
func GetTime() int64 {
	return time.Now().UnixNano()
}

// generate uuid
func GetUUID() string {
	id := uuid.New()
	return id.String()
}

func (s *HookService) GetEntityInfo(node string) *EntityInfo {
	if s.entities != nil {
		if entity, ok := s.entities[node]; ok{
			return entity
		}else{
			return nil
		}
	}
	return nil
}

//get decode data
func DecodeData(rawData []byte) interface{} {
	var data interface{}
	err := json.Unmarshal(rawData, &data)
	if nil != err {
		return ""
	}
	return data
}

func (s *HookService) OnMessagePublish(ctx context.Context, in *pb.MessagePublishRequest) (*pb.ValuedResponse, error) {
	res := &pb.ValuedResponse{}
	res.Type = pb.ValuedResponse_STOP_AND_RETURN
	res.Value = &pb.ValuedResponse_BoolResult{BoolResult: false}

	entity := s.GetEntityInfo(in.GetMessage().Node)
	if nil == entity {
		log.Warn("unknown message", in.GetMessage())
		return  res, nil
	}
	data := make(map[string]interface{})
	data["id"] = entity.EntityID
	data["owner"] = entity.Owner
	data["type"] = "device"
	data["source"] = "iothub"
	topic := in.GetMessage().Topic
	payload := DecodeData(in.GetMessage().GetPayload())
	if topic == RawDataTopic{
		data["data"] = map[string]interface{}{
			"rawData": map[string]interface{}{
				"timestamp": GetTime(),
				"topic": topic,
				"data": payload,
			},
		}
	}else if topic == AttributesTopic {
		data["data"] = map[string]interface{}{
			"attributes": map[string]interface{}{
				"timestamp": GetTime(),
				"topic": topic,
				"data": payload,
			},
		}

	}else if topic == TelemetryTopic {
		data["data"] = map[string]interface{}{
			"telemetry": map[string]interface{}{
				"timestamp": GetTime(),
				"topic": topic,
				"data": payload,
			},
		}
	}else if strings.HasPrefix(topic, AttributesTopicRequest){
		id := strings.Split(topic, AttributesTopicRequest)[0]
		log.Infof("get attribute id %s", id)
		// 边缘端获取平台属性值
		// todo 获取 payload keys, 向 core 查询 属性值， 返回给边端
	}else if strings.HasPrefix(topic, AttributesTopicResponse){
		id := strings.Split(topic, AttributesTopicResponse)[0]
		log.Infof("cmd response id %s", id)
		// 边缘端命令 response
		// todo 返回一般的 cmd ack 给到 tkeel-device or other application
	}else{
		log.Warnf("invalid topic %s", topic)
		return  res, errors.New("invalid topic")
	}
	log.Debug(data)
	if err := s.daprClient.PublishEvent(context.Background(), "iothub-pubsub", "core-pub", data); err != nil {
		log.Error(err)
		return  res, nil
	}
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

func (s *HookService) CreateSubscription(owner, devId string)(*v1.SubscriptionResponse, error){
	subId := GetUUID()
	subReq := &v1.SubscriptionObject{
		PubsubName: "iothub-pubsub",
		Topic: "sub-core",
		Mode: "onChange",
		Filter: fmt.Sprintf("insert into %s select %s.*", subId, devId),
		Source: "tkeel-device" ,
		Target: "iothub",
	}

	req, err := json.Marshal(subReq)
	if nil != err {
		log.Error(err)
		return nil, err
	}

	url :=  fmt.Sprintf("apis/core/v1/subscriptions?id=%s&source=%s&owner=%s", subId, "iothub", owner)
	res, err := s.daprClient.InvokeMethodWithContent(
		context.Background(),
		"keel",
		url,
		http.MethodPost,
		&dapr.DataContent{
			ContentType: "application/json",
			Data:        req,
		},
		)
	if err != nil {
		return nil, err
	}

	resp := new(v1.SubscriptionResponse)
	if err := json.Unmarshal(res, resp); nil != err{
		return nil, err
	}
	return resp, nil

}
