package service


type ConnectInfo struct {
	ClientID     string    `json:"_clientid"`
	PeerHost     string    `json:"_peerhost"`
	SocketPort   string    `json:"_socketport"`
	Protocol     string    `json:"_protocol"`
	UserName     string    `json:"_username"`
}

type EntityInfo struct {
	EntityID     string    `json:"entity_id"`
	Owner        string    `json:"owner"`
}


