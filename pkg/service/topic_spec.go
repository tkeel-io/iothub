package service

const (
	DeviceDebugTopic = "v1/devices/debug"
    RawDataTopic  = "v1/devices/me/raw"
    TelemetryTopic  = "v1/devices/me/telemetry"
    AttributesTopic string = "v1/devices/me/attributes"
    // CommandTopic commands
    CommandTopic string = "v1/devices/me/commands"
    CommandTopicResponse string = "v1/devices/me/command/response"
)
