package gateway

import (
	"context"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// MQTTCommand is a command message received by a terminal through MQTT.
type MQTTCommand struct {
	ClientID string
	Topic    string
	Payload  []byte
	QoS      byte
	Retained bool
}

// MQTTPublishResult describes an MQTT publish operation.
type MQTTPublishResult struct {
	ClientID string
	Topic    string
	QoS      byte
	Bytes    int
}

// DeviceUpload publishes terminal upload data through MQTT.
func DeviceUpload(ctx context.Context, cfg Config, payload []byte) (*MQTTPublishResult, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validateMQTT(); err != nil {
		return nil, err
	}

	client := mqtt.NewClient(mqttClientOptions(cfg, cfg.UploadClientID).
		SetCleanSession(false).
		SetAutoReconnect(true))
	if err := mqttConnect(ctx, client, cfg.OperationTimeout); err != nil {
		return nil, err
	}
	defer client.Disconnect(250)

	topic := cfg.UploadTopic()
	token := client.Publish(topic, cfg.MQTTQoS, false, payload)
	if err := waitMQTTToken(ctx, token, cfg.OperationTimeout, "mqtt publish"); err != nil {
		return nil, err
	}

	return &MQTTPublishResult{
		ClientID: cfg.UploadClientID,
		Topic:    topic,
		QoS:      cfg.MQTTQoS,
		Bytes:    len(payload),
	}, nil
}

// DeviceReceiveCommands subscribes terminal command messages through a
// persistent MQTT session. It blocks until ctx is cancelled or the handler
// returns an error.
func DeviceReceiveCommands(ctx context.Context, cfg Config, handler func(MQTTCommand) error) error {
	cfg = cfg.withDefaults()
	if err := cfg.validateMQTT(); err != nil {
		return err
	}
	if handler == nil {
		return fmt.Errorf("mqtt command handler is required")
	}

	client := mqtt.NewClient(mqttClientOptions(cfg, cfg.CommandClientID).
		SetCleanSession(false).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetResumeSubs(true))
	if err := mqttConnect(ctx, client, cfg.OperationTimeout); err != nil {
		return err
	}
	defer client.Disconnect(250)

	errCh := make(chan error, 1)
	topic := cfg.CommandTopic()
	token := client.Subscribe(topic, cfg.MQTTQoS, func(_ mqtt.Client, msg mqtt.Message) {
		cmd := MQTTCommand{
			ClientID: cfg.CommandClientID,
			Topic:    msg.Topic(),
			Payload:  append([]byte(nil), msg.Payload()...),
			QoS:      msg.Qos(),
			Retained: msg.Retained(),
		}
		if err := handler(cmd); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	})
	if err := waitMQTTToken(ctx, token, cfg.OperationTimeout, "mqtt subscribe"); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func mqttClientOptions(cfg Config, clientID string) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.MQTTBroker).
		SetClientID(clientID).
		SetUsername(cfg.MQTTUser).
		SetPassword(cfg.MQTTPassword).
		SetKeepAlive(cfg.MQTTKeepAlive)
	return opts
}

func mqttConnect(ctx context.Context, client mqtt.Client, timeout time.Duration) error {
	return waitMQTTToken(ctx, client.Connect(), timeout, "mqtt connect")
}

func waitMQTTToken(ctx context.Context, token mqtt.Token, timeout time.Duration, op string) error {
	done := make(chan struct{})
	go func() {
		token.Wait()
		close(done)
	}()

	var timer <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		timer = t.C
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer:
		return fmt.Errorf("%s timeout after %s", op, timeout)
	case <-done:
		if err := token.Error(); err != nil {
			return fmt.Errorf("%s failed: %w", op, err)
		}
		return nil
	}
}
