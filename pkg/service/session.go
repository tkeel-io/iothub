package service

type ConnectInfo struct {
    ClientID   string `json:"_clientId"`
    PeerHost   string `json:"_peerHost"`
    SocketPort string `json:"_sockPort"`
    Protocol   string `json:"_protocol"`
    UserName   string `json:"_userName"`
    Online     bool   `json:"_online"`
    Owner      string `json:"_owner"`
    Timestamp  int64  `json:"_timestamp"`
}

type DeviceEntityInfo struct {
    EntityID string `json:"entity_id"`
    Owner    string `json:"owner"`
}
