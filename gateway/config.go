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

	// UploadTopicPrefix is the MQTT topic prefix for terminal uploads.
	// The default upload topic is "<UploadTopicPrefix>/<deviceID>".
	UploadTopicPrefix string

	// CommandTopicPrefix is the MQTT topic prefix for platform commands.
	// The default command topic is "<CommandTopicPrefix>/<deviceID>".
	CommandTopicPrefix string

	// UploadFilter is used by the NATS backend to receive terminal uploads.
	// When empty, it defaults to "<UploadTopicPrefix>/#".
	UploadFilter string

	// CommandClientID identifies the MQTT persistent session used to receive
	// platform commands. When empty, the deviceID passed to
	// DeviceReceiveCommands is used.
	CommandClientID string

	// UploadClientID identifies the MQTT upload simulator/client. When empty,
	// "<deviceID>_upload" is used.
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

// DefaultConfig returns topic and timeout defaults. Connection addresses,
// credentials and operation device IDs should be filled by the caller.
func DefaultConfig() Config {
	return Config{
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

func (c Config) withDevice(deviceID string) (Config, string, error) {
	c = c.withDefaults()
	deviceID = strings.TrimSpace(deviceID)
	if deviceID == "" {
		return c, "", fmt.Errorf("device id is required")
	}
	if c.CommandClientID == "" {
		c.CommandClientID = deviceID
	}
	if c.UploadClientID == "" {
		c.UploadClientID = deviceID + "_upload"
	}
	return c, deviceID, nil
}

// UploadTopic returns the terminal upload MQTT topic for deviceID.
func (c Config) UploadTopic(deviceID string) string {
	c = c.withDefaults()
	return joinTopic(c.UploadTopicPrefix, deviceID)
}

// CommandTopic returns the platform command MQTT topic for deviceID.
func (c Config) CommandTopic(deviceID string) string {
	c = c.withDefaults()
	return joinTopic(c.CommandTopicPrefix, deviceID)
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
	if err := c.validateQoS(); err != nil {
		return err
	}
	return nil
}

func (c Config) validateNATS() error {
	if c.NATSURL == "" {
		return fmt.Errorf("nats url is required")
	}
	return nil
}

func (c Config) validateQoS() error {
	if c.MQTTQoS > 2 {
		return fmt.Errorf("mqtt qos must be 0, 1 or 2")
	}
	return nil
}

func joinTopic(prefix, suffix string) string {
	return strings.TrimSuffix(prefix, "/") + "/" + strings.TrimPrefix(suffix, "/")
}
