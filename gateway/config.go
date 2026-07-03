package gateway

import (
	"fmt"
	"strings"
	"time"
)

const (
	DefaultUploadTopicPrefix  = "WT1_receive"
	DefaultCommandTopicPrefix = "WT1_service_parameter"
	DefaultMQTTStream         = "$MQTT_msgs"
	DefaultMQTTSubjectPrefix  = "$MQTT.msgs."
	DefaultBackendDurable     = "backend_mqtt_consumer"

	mqttQoSHeader     = "Nmqtt-Pub"
	mqttSubjectHeader = "Nmqtt-Subject"
	mqttMappedHeader  = "Nmqtt-Mapped"
)

// Config contains all network, topic and persistence settings used by the
// MQTT/NATS gateway helper functions.
type Config struct {
	NATSURL      string
	NATSUser     string
	NATSPassword string

	MQTTBroker   string
	MQTTUser     string
	MQTTPassword string

	DeviceID string

	// UploadTopicPrefix is the MQTT topic prefix for terminal uploads.
	// The default upload topic is "<UploadTopicPrefix>/<DeviceID>".
	UploadTopicPrefix string

	// CommandTopicPrefix is the MQTT topic prefix for platform commands.
	// The default command topic is "<CommandTopicPrefix>/<DeviceID>".
	CommandTopicPrefix string

	// UploadFilter is used by the NATS backend to receive terminal uploads.
	// When empty, it defaults to "<UploadTopicPrefix>/#".
	UploadFilter string

	// CommandClientID identifies the MQTT persistent session used to receive
	// platform commands. When empty, DeviceID is used.
	CommandClientID string

	// UploadClientID identifies the MQTT upload simulator/client. When empty,
	// "<DeviceID>_upload" is used.
	UploadClientID string

	MQTTQoS          byte
	MQTTKeepAlive    time.Duration
	ConnectTimeout   time.Duration
	OperationTimeout time.Duration

	MQTTStream        string
	MQTTSubjectPrefix string
	BackendDurable    string
	NATSNamePrefix    string
}

// DefaultConfig returns topic and timeout defaults for a device. Connection
// addresses and credentials should be filled by the caller.
func DefaultConfig(deviceID string) Config {
	return Config{
		DeviceID:           deviceID,
		UploadTopicPrefix:  DefaultUploadTopicPrefix,
		CommandTopicPrefix: DefaultCommandTopicPrefix,
		MQTTQoS:            1,
		MQTTKeepAlive:      300 * time.Second,
		ConnectTimeout:     5 * time.Second,
		OperationTimeout:   5 * time.Second,
		MQTTStream:         DefaultMQTTStream,
		MQTTSubjectPrefix:  DefaultMQTTSubjectPrefix,
		BackendDurable:     DefaultBackendDurable,
		NATSNamePrefix:     "gateway",
	}
}

func (c Config) withDefaults() Config {
	if c.UploadTopicPrefix == "" {
		c.UploadTopicPrefix = DefaultUploadTopicPrefix
	}
	if c.CommandTopicPrefix == "" {
		c.CommandTopicPrefix = DefaultCommandTopicPrefix
	}
	if c.UploadFilter == "" {
		c.UploadFilter = joinTopic(c.UploadTopicPrefix, "#")
	}
	if c.CommandClientID == "" {
		c.CommandClientID = c.DeviceID
	}
	if c.UploadClientID == "" && c.DeviceID != "" {
		c.UploadClientID = c.DeviceID + "_upload"
	}
	if c.MQTTQoS == 0 {
		c.MQTTQoS = 1
	}
	if c.MQTTKeepAlive == 0 {
		c.MQTTKeepAlive = 300 * time.Second
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 5 * time.Second
	}
	if c.OperationTimeout == 0 {
		c.OperationTimeout = 5 * time.Second
	}
	if c.MQTTStream == "" {
		c.MQTTStream = DefaultMQTTStream
	}
	if c.MQTTSubjectPrefix == "" {
		c.MQTTSubjectPrefix = DefaultMQTTSubjectPrefix
	}
	if c.BackendDurable == "" {
		c.BackendDurable = DefaultBackendDurable
	}
	if c.NATSNamePrefix == "" {
		c.NATSNamePrefix = "gateway"
	}
	return c
}

// UploadTopic returns the terminal upload MQTT topic for this config.
func (c Config) UploadTopic() string {
	c = c.withDefaults()
	return joinTopic(c.UploadTopicPrefix, c.DeviceID)
}

// CommandTopic returns the platform command MQTT topic for this config.
func (c Config) CommandTopic() string {
	c = c.withDefaults()
	return joinTopic(c.CommandTopicPrefix, c.DeviceID)
}

// BackendUploadFilter returns the MQTT topic filter used by the NATS backend
// to consume upload messages from the MQTT gateway stream.
func (c Config) BackendUploadFilter() string {
	c = c.withDefaults()
	return c.UploadFilter
}

func (c Config) validateMQTT() error {
	if c.MQTTBroker == "" {
		return fmt.Errorf("mqtt broker is required")
	}
	if c.DeviceID == "" {
		return fmt.Errorf("device id is required")
	}
	return nil
}

func (c Config) validateNATS() error {
	if c.NATSURL == "" {
		return fmt.Errorf("nats url is required")
	}
	return nil
}

func joinTopic(prefix, suffix string) string {
	return strings.TrimSuffix(prefix, "/") + "/" + strings.TrimPrefix(suffix, "/")
}
