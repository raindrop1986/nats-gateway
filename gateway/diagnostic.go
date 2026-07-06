package gateway

import (
	"context"
	"errors"
	"strings"

	natsgo "github.com/nats-io/nats.go"
)

// Diagnostic contains a snapshot of the MQTT gateway JetStream state.
type Diagnostic struct {
	Stream    *StreamSnapshot
	Consumers []ConsumerSnapshot
	Messages  []StoredMessageSnapshot
}

type StreamSnapshot struct {
	Name      string
	Subjects  []string
	Messages  uint64
	Bytes     uint64
	FirstSeq  uint64
	LastSeq   uint64
	Consumers int
}

type ConsumerSnapshot struct {
	Name            string
	Durable         string
	DeliverSubject  string
	FilterSubject   string
	FilterSubjects  []string
	DeliveredStream uint64
	AckFloorStream  uint64
	Pending         uint64
	AckPending      int
}

type StoredMessageSnapshot struct {
	Sequence    uint64
	Subject     string
	Topic       string
	QoS         string
	MQTTSubject string
	MQTTMapped  string
	Bytes       int
}

// DiagnoseMQTTStream returns a small snapshot of the internal MQTT stream,
// MQTT session consumers and the latest stored messages.
func DiagnoseMQTTStream(ctx context.Context, cfg Config, latest int) (*Diagnostic, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validateNATS(); err != nil {
		return nil, err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.OperationTimeout*3)
		defer cancel()
	}
	if latest <= 0 {
		latest = 5
	}

	nc, js, err := connectNATS(cfg, "diag")
	if err != nil {
		return nil, err
	}
	defer nc.Close()

	info, err := js.StreamInfo(cfg.MQTTStream, natsgo.Context(ctx))
	if errors.Is(err, natsgo.ErrStreamNotFound) {
		return &Diagnostic{}, nil
	}
	if err != nil {
		return nil, err
	}

	out := &Diagnostic{
		Stream: &StreamSnapshot{
			Name:      info.Config.Name,
			Subjects:  append([]string(nil), info.Config.Subjects...),
			Messages:  info.State.Msgs,
			Bytes:     info.State.Bytes,
			FirstSeq:  info.State.FirstSeq,
			LastSeq:   info.State.LastSeq,
			Consumers: info.State.Consumers,
		},
	}

	for ci := range js.Consumers(cfg.MQTTStream, natsgo.Context(ctx)) {
		out.Consumers = append(out.Consumers, ConsumerSnapshot{
			Name:            ci.Name,
			Durable:         ci.Config.Durable,
			DeliverSubject:  ci.Config.DeliverSubject,
			FilterSubject:   ci.Config.FilterSubject,
			FilterSubjects:  append([]string(nil), ci.Config.FilterSubjects...),
			DeliveredStream: ci.Delivered.Stream,
			AckFloorStream:  ci.AckFloor.Stream,
			Pending:         ci.NumPending,
			AckPending:      ci.NumAckPending,
		})
	}

	if info.State.LastSeq == 0 {
		return out, nil
	}

	start := info.State.FirstSeq
	latestSeq := uint64(latest)
	if info.State.LastSeq > latestSeq && info.State.LastSeq-latestSeq+1 > start {
		start = info.State.LastSeq - latestSeq + 1
	}
	for seq := start; seq <= info.State.LastSeq; seq++ {
		msg, err := js.GetMsg(cfg.MQTTStream, seq, natsgo.Context(ctx))
		if err != nil {
			continue
		}
		businessSubject := strings.TrimPrefix(msg.Subject, cfg.MQTTSubjectPrefix)
		out.Messages = append(out.Messages, StoredMessageSnapshot{
			Sequence:    msg.Sequence,
			Subject:     msg.Subject,
			Topic:       natsSubjectToMQTTTopic(businessSubject),
			QoS:         msg.Header.Get(mqttQoSHeader),
			MQTTSubject: msg.Header.Get(mqttSubjectHeader),
			MQTTMapped:  msg.Header.Get(mqttMappedHeader),
			Bytes:       len(msg.Data),
		})
	}

	return out, nil
}
