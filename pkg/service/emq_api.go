package service

import (
	"bytes"
	"errors"
	"github.com/tkeel-io/kit/log"
	"io/ioutil"
	"net/http"
	json "encoding/json"
)

// emq deployed address
const ServerAddress string = "http://192.168.100.5:30855/api"

// base64 encode: admin:public --> YWRtaW46cHVibGlj
const AuthorizationValue string = "Basic YWRtaW46cHVibGlj"

// emq info type
const ClientsInfo string = "client"
const SubscribeTopicsInfo string = "subscribe"

// get emq info
func GetEmqInfo(infoType string) ([]map[string]interface{}, error) {
	var url string
	if infoType == ClientsInfo {
		url = ServerAddress + "/v4/clients?_page=1&_limit=100000"
	} else if infoType == SubscribeTopicsInfo {
		url = ServerAddress + "/v4/subscriptions?_page=1&_limit=100000"
	} else {
		return nil, errors.New("invalid infoType")
	}

	req, err := http.NewRequest("GET", url, nil)
	if nil != err {
		return nil, err
	}

	req.Header.Add("Authorization", AuthorizationValue)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error("error ", err)
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error("error ReadAll", err)
		return nil, err
	}
	log.Debug("receive resp, ", string(body))
	if resp.StatusCode != 200 {
		log.Error("bad status ", resp.StatusCode)
		return nil, errors.New(resp.Status)
	}

	var result interface{}
	if err = json.Unmarshal(body, &result); nil != err {
		log.Error("body Unmarshal error", err)
		return nil, err
	}
	res, ok := result.(map[string]interface{})
	if !ok {
		return nil, errors.New("result error")
	}
	if res["code"].(float64) != 0 {
		return nil, errors.New("invalid code")
	}

	data := res["data"].([]map[string]interface{})
	return data, nil
}

func Publish(username, topic, clientId string, qos int, retain bool, payload interface{}) error {
	url := ServerAddress + "/api/v4/mqtt/publish"
	pubData := map[string]interface{}{
		"topic": username + "/" + topic,
		"payload": payload,
		"qos": qos,
		"retain": retain,
		"clientid": clientId,
	}
	data, err := json.Marshal(pubData)
	if nil != err {
		log.Error("error ", err)
		return err
	}
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Error("error ", err)
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error("error ReadAll", err)
		return err
	}
	log.Debug("receive resp, ", string(body))
	if resp.StatusCode != 200 {
		log.Error("bad status ", resp.StatusCode)
		return errors.New(resp.Status)
	}

	var result interface{}
	if err = json.Unmarshal(body, &result); nil != err {
		log.Error("body Unmarshal error", err)
		return err
	}
	res, ok := result.(map[string]interface{})
	if !ok {
		return errors.New("result error")
	}
	if res["code"].(float64) != 0 {
		return errors.New("invalid code")
	}
	return nil
}
