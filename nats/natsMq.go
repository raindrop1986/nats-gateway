package nats

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"nast-gateway/gateway"
	"os"
	"os/signal"
	"strings"
)

const (
	natsURL      = "nats://192.168.1.192:4222"
	natsUser     = "nats_backend"
	natsPassword = "nats@2026"
	deviceID     = "WT260605135206"
)

func NatsSubPub(mode string) {
	switch mode {
	case "sub":
		natsSubscribeUploads()
	case "pub":
		natsPublishCommand()
	case "diag":
		natsDiagnose()
	default:
		log.Fatalf("unknown mode %q, want sub, pub or diag", mode)
	}
}

func natsSubscribeUploads() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	cfg := sampleConfig()
	log.Printf("nats subscribing upload filter=%s durable=%s", cfg.BackendUploadFilter(), cfg.BackendDurable)
	err := gateway.NATSSubscribeUploads(ctx, cfg, func(msg gateway.NATSUpload) error {
		fmt.Printf("nats received from mqtt stream subject=%s mqtt_topic=%s qos=%s payload=%s\n",
			msg.Subject,
			msg.Topic,
			msg.QoS,
			strings.ToUpper(hex.EncodeToString(msg.Payload)),
		)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("nats subscribe failed: %v", err)
	}
	log.Println("nats subscriber exiting")
}

func natsPublishCommand() {
	payload, err := hexPayload()
	if err != nil {
		log.Fatal(err)
	}

	result, err := gateway.NATSPublishCommand(context.Background(), sampleConfig(), payload)
	if err != nil {
		log.Fatalf("nats publish failed: %v", err)
	}
	log.Printf("nats published command stream=%s seq=%d subject=%s mqtt_topic=%s bytes=%d",
		result.Stream,
		result.Sequence,
		result.Subject,
		result.Topic,
		result.Bytes,
	)
}

func natsDiagnose() {
	diag, err := gateway.DiagnoseMQTTStream(context.Background(), sampleConfig(), 5)
	if err != nil {
		log.Fatalf("nats diagnose failed: %v", err)
	}
	if diag.Stream == nil {
		log.Printf("mqtt stream does not exist")
		return
	}

	log.Printf("stream=%s subjects=%v messages=%d bytes=%d first_seq=%d last_seq=%d consumers=%d",
		diag.Stream.Name,
		diag.Stream.Subjects,
		diag.Stream.Messages,
		diag.Stream.Bytes,
		diag.Stream.FirstSeq,
		diag.Stream.LastSeq,
		diag.Stream.Consumers,
	)
	if len(diag.Consumers) == 0 {
		log.Printf("no consumer found on mqtt stream")
	}
	for _, consumer := range diag.Consumers {
		log.Printf("consumer=%s durable=%s deliver=%s filter=%s filters=%v delivered_stream=%d ack_floor_stream=%d pending=%d ack_pending=%d",
			consumer.Name,
			consumer.Durable,
			consumer.DeliverSubject,
			consumer.FilterSubject,
			consumer.FilterSubjects,
			consumer.DeliveredStream,
			consumer.AckFloorStream,
			consumer.Pending,
			consumer.AckPending,
		)
	}
	for _, msg := range diag.Messages {
		log.Printf("msg seq=%d subject=%s mqtt_topic=%s qos=%s mqtt_subject=%s mqtt_mapped=%s bytes=%d",
			msg.Sequence,
			msg.Subject,
			msg.Topic,
			msg.QoS,
			msg.MQTTSubject,
			msg.MQTTMapped,
			msg.Bytes,
		)
	}
}

func sampleConfig() gateway.Config {
	cfg := gateway.DefaultConfig(deviceID)
	cfg.NATSURL = natsURL
	cfg.NATSUser = natsUser
	cfg.NATSPassword = natsPassword
	return cfg
}

func hexPayload() ([]byte, error) {
	msg := "F5 5A 02 44 0F 4C 09 0C 38 39 38 35 32 30 30 30 32 36 33 33 32 31 34 30 37 37 39 35 56 32 30 32 35 30 36 30 34 56 31 2E 30 2E 30 00 13 00 00 20 CC 01 01 00 D6 B3 00 00 B7 9B DF 02 8D F9 5A 68 EB 58 2F 0B F5 5A 01 14 8D F9 5A 68 A4 8C 70 41 4B 6A C2 42 18 6F 7E 37 AA"
	return hex.DecodeString(strings.ReplaceAll(msg, " ", ""))
}
