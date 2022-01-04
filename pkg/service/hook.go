package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	dapr "github.com/dapr/go-sdk/client"

	"github.com/google/uuid"
	"github.com/pkg/errors"
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
	connectInfoProperty = `connectinfo`

	// default client id for cloud
	defaultDownStreamClientId = `@tkeel.iothub.internal.clienId`

	// default header key
	tkeelAuthHeader = `x-tKeel-auth`
	defaultTenant   = `_tKeel_system`
	defaultUser     = `_tKeel_admin`
	defultRole      = `admin`

	// default base url
	BaseUrl         = `http://localhost:3500/v1.0/invoke/keel/method/apis`
)

// HookService is used to implement emqx_exhook_v1.s *HookService.
type HookService struct {
	pb.UnimplementedHookProviderServer
	daprClient dapr.Client
	//clients map[string]*ConnectInfo
	//entities map[string]*DeviceEntityInfo

	// map["clientid"][{"topic": "xxx/xxx", "qos": 0, "node": "XXX"},]
	subscribeTopics map[string][]map[string]interface{}
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
	username := in.Clientinfo.Username
	ci := &ConnectInfo{
		ClientID:   in.Clientinfo.Clientid,
		UserName:   in.Clientinfo.Username,
		PeerHost:   in.Clientinfo.Peerhost,
		Protocol:   in.Clientinfo.Protocol,
		SocketPort: strconv.Itoa(int(in.Clientinfo.Sockport)),
		Online:     true,
	}
	infoMap := map[string]interface{}{
		connectInfoProperty: *ci,
	}
	// get owner
	owner, err := s.GetState(username, devEntitySuffixKey)
	if err != nil {
		return nil, err
	}

	data := map[string]interface{}{
		"id":     username,
		"owner":  string(owner),
		"type":   "device",
		"source": "iothub",
		"data":   infoMap,
	}
	log.Debugf("pub data %s", data)
	if err := s.daprClient.PublishEvent(context.Background(), "iothub-pubsub", "core-pub", data); err != nil {
		log.Error(err)
		return nil, err
	}
	//save ConnectInfo
	ciByte, err := json.Marshal(ci)
	if nil != err {
		log.Error(err)
		return nil, err
	}
	if err := s.SaveState(username, connectInfoSuffixKey, ciByte); err != nil {
		return nil, err
	}

	return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnClientDisconnected(ctx context.Context, in *pb.ClientDisconnectedRequest) (*pb.EmptySuccess, error) {
	username := in.Clientinfo.Username
	ci := &ConnectInfo{
		ClientID:   "",
		UserName:   "",
		PeerHost:   "",
		Protocol:   "",
		SocketPort: "",
		Online:     false,
	}
	infoMap := map[string]interface{}{
		connectInfoProperty: *ci,
	}
	// get owner
	owner, err := s.GetState(username, devEntitySuffixKey)
	if err != nil {
		return nil, err
	}
	data := map[string]interface{}{
		"id":     username,
		"owner":  string(owner),
		"type":   "device",
		"source": "iothub",
		"data":   infoMap,
	}
	log.Debugf("pub data %s", data)
	if err := s.daprClient.PublishEvent(context.Background(), "iothub-pubsub", "core-pub", data); err != nil {
		log.Error(err)
		return nil, err
	}

	//delete connect info from state store
	if err := s.DeleteState(username, connectInfoSuffixKey); err != nil {
		log.Errorf("Failed to delete state store: %v", err)
		return nil, err
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
	if (tokenResp.Code != 200) || (tokenResp.Data.EntityID != username) {
		log.Errorf("auth result code: %v or invalid username %s", tokenResp.Msg, username)
		return false
	}
	//save owner
	if err := s.SaveState(username, devEntitySuffixKey, []byte(tokenResp.Data.Owner)); err != nil {
		return false
	}
	return true
}

func (s *HookService) OnClientAuthenticate(ctx context.Context, in *pb.ClientAuthenticateRequest) (*pb.ValuedResponse, error) {
	res := &pb.ValuedResponse{}
	res.Type = pb.ValuedResponse_STOP_AND_RETURN
	log.Debug(in.GetClientinfo())
	authRes := s.auth(in.Clientinfo.GetPassword(), in.Clientinfo.GetUsername())
	res.Value = &pb.ValuedResponse_BoolResult{BoolResult: authRes}
	return res, nil
}

func (s *HookService) OnClientCheckAcl(ctx context.Context, in *pb.ClientCheckAclRequest) (*pb.ValuedResponse, error) { //nolint
	return &pb.ValuedResponse{}, nil
}

func (s *HookService) OnClientSubscribe(ctx context.Context, in *pb.ClientSubscribeRequest) (*pb.EmptySuccess, error) {
	topics := in.GetTopicFilters()
	for _, tf := range topics {
		topic := tf.GetName()
		username := getUserNameFromTopic(topic)
		//get owner
		value, err := s.GetState(username, devEntitySuffixKey)
		if err != nil {
			return nil, err
		}
		owner := string(value)

		log.Debugf("client subscribe topic: %s, username: %s", topic, username)
		// 创建 core 订阅实体
		if topic == AttributesTopic {
			s.CreateSubscribeEntity(owner, username, attributeProperty)

		} else if topic == TelemetryTopic {
			s.CreateSubscribeEntity(owner, username, telemetryProperty)

		} else if topic == CommandTopicRequest {
			//订阅平台命令
			//do nothing
			s.CreateSubscribeEntity(owner, username, commandProperty)
		} else if topic == (AttributesTopicResponse + "+") {
			//边端订阅平台属性
			//do nothing
		} else {
			return nil, errors.New("invalid topic")
		}

	}
	return &pb.EmptySuccess{}, nil
}

func (s *HookService) OnClientUnsubscribe(ctx context.Context, in *pb.ClientUnsubscribeRequest) (*pb.EmptySuccess, error) {
	topics := in.GetTopicFilters()
	for _, tf := range topics {
		topic := tf.GetName()
		username := getUserNameFromTopic(topic)
		//get owner
		owner, err := s.GetState(username, devEntitySuffixKey)
		if err != nil {
			return nil, err
		}
		// get subscription Id
		subId, err := s.GetState(username, subEntitySuffixKey)
		if err != nil {
			return nil, err
		}

		// 删除 core 订阅实体
		if err := s.DeleteSubscribeEntity(string(owner), username, string(subId)); nil != err {
			return nil, err
		}
	}
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

//get decode data
func DecodeData(rawData []byte) interface{} {
	var data interface{}
	err := json.Unmarshal(rawData, &data)
	if nil != err {
		return ""
	}
	return data
}

func getUserNameFromTopic(topic string) (user string) {
	items := strings.SplitN(topic, "/", 2)
	if len(items) != 2 {
		return
	}
	return items[0]
}

func (s *HookService) OnMessagePublish(ctx context.Context, in *pb.MessagePublishRequest) (*pb.ValuedResponse, error) {
	res := &pb.ValuedResponse{}
	res.Type = pb.ValuedResponse_STOP_AND_RETURN
	res.Value = &pb.ValuedResponse_BoolResult{BoolResult: false}
	//do nothing when receive tkeel attribute/telemetry/command event.
	if in.Message.From == defaultDownStreamClientId {
		log.Debugf("downstream data: %v", in.GetMessage())
		res.Value = &pb.ValuedResponse_BoolResult{BoolResult: true}
		return res, nil
	}

	username := getUserNameFromTopic(in.Message.Topic)
	//get owner
	owner, err := s.GetState(username, devEntitySuffixKey)
	if err != nil {
		return nil, err
	}
	data := make(map[string]interface{})
	data["id"] = username
	data["owner"] = string(owner)
	data["type"] = "device"
	data["source"] = "iothub"
	topic := in.GetMessage().Topic
	payload := DecodeData(in.GetMessage().GetPayload())
	if topic == (username + "/" + RawDataTopic) {
		data["data"] = map[string]interface{}{
			rawDataProperty: map[string]interface{}{
				"timestamp": GetTime(),
				"topic":     topic,
				"data":      payload,
			},
		}
	} else if topic == (username + "/" + AttributesTopic) {
		data["data"] = map[string]interface{}{
			attributeProperty: map[string]interface{}{
				"timestamp": GetTime(),
				"topic":     topic,
				"data":      payload,
			},
		}

	} else if topic == (username + "/" + TelemetryTopic) {
		data["data"] = map[string]interface{}{
			telemetryProperty: map[string]interface{}{
				"timestamp": GetTime(),
				"topic":     topic,
				"data":      payload,
			},
		}
	} else if strings.HasPrefix(topic, username+"/"+AttributesTopicRequest) {
		id := strings.Split(topic, username+"/"+AttributesTopicRequest)[0]
		log.Infof("get attribute id %s", id)
		// 边缘端获取平台属性值
		// todo 获取 payload keys, 向 core 查询 属性值， 返回给边端
	} else if strings.HasPrefix(topic, username+"/"+AttributesTopicResponse) {
		id := strings.Split(topic, username+"/"+AttributesTopicResponse)[0]
		log.Infof("cmd response id %s", id)
		// 边缘端命令 response
		// todo 返回一般的 cmd ack 给到 tkeel-device or other application
	} else {
		log.Warnf("invalid topic %s", topic)
		return res, errors.New("invalid topic")
	}
	log.Debug(data)
	if err := s.daprClient.PublishEvent(context.Background(), "iothub-pubsub", "core-pub", data); err != nil {
		log.Error(err)
		return res, nil
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

func AddDefaultAuthHeader(req *http.Request) {
	authString := fmt.Sprintf("tenant=%s&user=%s&role=%s", defaultTenant, defaultUser, defultRole)
	req.Header.Add(tkeelAuthHeader, base64.StdEncoding.EncodeToString([]byte(authString)))
}

// create SubscribeEntity
func (s *HookService) CreateSubscribeEntity(owner, devId, itemType string) error {
	subId := GetUUID()
	subReq := &v1.SubscriptionObject{
		PubsubName: "iothub-pubsub",
		Topic:      "sub-core",
		Mode:       "realtime", //fixme: it should be "onChange"
		Filter:     fmt.Sprintf("insert into %s select %s.%s", subId, devId, itemType),
		Source:     "tkeel-device",
		Target:     "iothub",
	}

	data, err := json.Marshal(subReq)
	if nil != err {
		log.Error(err)
		return err
	}

	url := fmt.Sprintf(BaseUrl + "/core/v1/subscriptions?id=%s&source=%s&owner=%s&type=%s", subId, "iothub", owner, "SUBSCRIPTION")
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

	log.Debugf("create subscription ok, %v", res)
	//save subId
	if err := s.SaveState(devId, subEntitySuffixKey, []byte(subId)); err != nil {
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
func (s *HookService) GetState(username, suffixKey string) ([]byte, error) {
	item, err := s.daprClient.GetState(context.Background(), iothubPrivateStatesStoreName, username+suffixKey)
	if err != nil {
		log.Errorf("Failed to get state: %v", err)
		return nil, err
	}
	return item.Value, nil
}

// save state store
func (s *HookService) SaveState(username, suffixKey string, data []byte) error {
	if err := s.daprClient.SaveState(context.Background(), iothubPrivateStatesStoreName, username+suffixKey, data); err != nil {
		log.Errorf("Failed to persist state: %v\n", err)
		return err
	}
	return nil
}

// delete state store
func (s *HookService) DeleteState(username, suffixKey string) error {
	if err := s.daprClient.DeleteState(context.Background(), iothubPrivateStatesStoreName, username+suffixKey); err != nil {
		log.Errorf("Failed to delete state store: %v", err)
		return err
	}
	return nil
}
