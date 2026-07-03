package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"

	natsgo "github.com/nats-io/nats.go"
)

// NATSUpload is an upload message consumed by the platform through NATS.
type NATSUpload struct {
	Subject string
	Topic   string
	Payload []byte
	QoS     string
	Header  natsgo.Header
}

// NATSPublishResult describes a platform command published through NATS into
// the MQTT gateway persistence stream.
type NATSPublishResult struct {
	Stream   string
	Sequence uint64
	Subject  string
	Topic    string
	Bytes    int
}

// NATSSubscribeUploads consumes terminal upload messages from the MQTT gateway
// stream through a durable JetStream consumer. It blocks until ctx is cancelled
// or the handler returns an error.
func NATSSubscribeUploads(ctx context.Context, cfg Config, handler func(NATSUpload) error) error {
	cfg = cfg.withDefaults()
	if err := cfg.validateNATS(); err != nil {
		return err
	}
	if handler == nil {
		return fmt.Errorf("nats upload handler is required")
	}

	nc, js, err := connectNATS(cfg, "upload-sub")
	if err != nil {
		return err
	}
	defer nc.Close()

	if err := ensureMQTTMsgStream(js, cfg); err != nil {
		return err
	}

	errCh := make(chan error, 1)
	filterSubject := cfg.MQTTSubjectPrefix + mqttTopicFilterToNATSSubject(cfg.BackendUploadFilter())
	_, err = js.Subscribe(filterSubject, func(msg *natsgo.Msg) {
		businessSubject := strings.TrimPrefix(msg.Subject, cfg.MQTTSubjectPrefix)
		upload := NATSUpload{
			Subject: businessSubject,
			Topic:   natsSubjectToMQTTTopic(businessSubject),
			Payload: append([]byte(nil), msg.Data...),
			QoS:     msg.Header.Get(mqttQoSHeader),
			Header:  msg.Header,
		}
		if err := handler(upload); err != nil {
			select {
			case errCh <- err:
			default:
			}
			return
		}
		if err := msg.Ack(); err != nil {
			select {
			case errCh <- fmt.Errorf("ack upload message: %w", err):
			default:
			}
		}
	},
		natsgo.BindStream(cfg.MQTTStream),
		natsgo.Durable(cfg.BackendDurable),
		natsgo.ManualAck(),
		natsgo.DeliverAll(),
	)
	if err != nil {
		return fmt.Errorf("subscribe mqtt upload stream: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

// NATSPublishCommand publishes a platform command into the MQTT gateway stream
// so a persistent MQTT session can receive it after reconnecting.
func NATSPublishCommand(ctx context.Context, cfg Config, payload []byte) (*NATSPublishResult, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validateNATS(); err != nil {
		return nil, err
	}
	if cfg.DeviceID == "" {
		return nil, fmt.Errorf("device id is required")
	}

	nc, js, err := connectNATS(cfg, "command-pub")
	if err != nil {
		return nil, err
	}
	defer nc.Close()

	if err := ensureMQTTMsgStream(js, cfg); err != nil {
		return nil, err
	}

	commandTopic := cfg.CommandTopic()
	natsSubject, err := mqttTopicToNATSSubject(commandTopic)
	if err != nil {
		return nil, err
	}

	msg := natsgo.NewMsg(cfg.MQTTSubjectPrefix + natsSubject)
	msg.Header.Set(mqttQoSHeader, fmt.Sprintf("%d", cfg.MQTTQoS))
	msg.Header.Set(mqttSubjectHeader, natsSubject)
	msg.Header.Set(mqttMappedHeader, natsSubject)
	msg.Data = payload

	ack, err := js.PublishMsg(msg, natsgo.AckWait(cfg.OperationTimeout), natsgo.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("publish mqtt command stream: %w", err)
	}

	return &NATSPublishResult{
		Stream:   ack.Stream,
		Sequence: ack.Sequence,
		Subject:  natsSubject,
		Topic:    commandTopic,
		Bytes:    len(payload),
	}, nil
}

func connectNATS(cfg Config, name string) (*natsgo.Conn, natsgo.JetStreamContext, error) {
	opts := []natsgo.Option{
		natsgo.Name(cfg.NATSNamePrefix + "-" + name),
		natsgo.Timeout(cfg.ConnectTimeout),
	}
	if cfg.NATSUser != "" || cfg.NATSPassword != "" {
		opts = append(opts, natsgo.UserInfo(cfg.NATSUser, cfg.NATSPassword))
	}

	nc, err := natsgo.Connect(cfg.NATSURL, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("connect nats: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("init jetstream: %w", err)
	}
	return nc, js, nil
}

func ensureMQTTMsgStream(js natsgo.JetStreamContext, cfg Config) error {
	_, err := js.StreamInfo(cfg.MQTTStream)
	if err == nil {
		return nil
	}
	if !errors.Is(err, natsgo.ErrStreamNotFound) {
		return fmt.Errorf("get mqtt stream info: %w", err)
	}

	_, err = js.AddStream(&natsgo.StreamConfig{
		Name:      cfg.MQTTStream,
		Subjects:  []string{cfg.MQTTSubjectPrefix + ">"},
		Storage:   natsgo.FileStorage,
		Retention: natsgo.LimitsPolicy,
	})
	if err != nil {
		return fmt.Errorf("create mqtt stream: %w", err)
	}
	return nil
}

func natsSubjectToMQTTTopic(subject string) string {
	return strings.ReplaceAll(subject, ".", "/")
}

func mqttTopicToNATSSubject(topic string) (string, error) {
	if strings.ContainsAny(topic, "#+") {
		return "", fmt.Errorf("publish topic %q cannot contain MQTT wildcards", topic)
	}
	return strings.ReplaceAll(topic, "/", "."), nil
}

func mqttTopicFilterToNATSSubject(filter string) string {
	subject := strings.ReplaceAll(filter, "/", ".")
	subject = strings.ReplaceAll(subject, "#", ">")
	subject = strings.ReplaceAll(subject, "+", "*")
	return subject
}
