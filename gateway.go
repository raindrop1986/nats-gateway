package gateway

import (
	"context"

	core "github.com/raindrop1986/nats-gateway/gateway"
)

const (
	DefaultUploadTopicPrefix  = core.DefaultUploadTopicPrefix
	DefaultCommandTopicPrefix = core.DefaultCommandTopicPrefix
	DefaultMQTTStream         = core.DefaultMQTTStream
	DefaultMQTTSubjectPrefix  = core.DefaultMQTTSubjectPrefix
	DefaultBackendDurable     = core.DefaultBackendDurable
)

type Config = core.Config

type MQTTCommand = core.MQTTCommand
type MQTTPublishResult = core.MQTTPublishResult

type NATSUpload = core.NATSUpload
type NATSPublishResult = core.NATSPublishResult

type Diagnostic = core.Diagnostic
type StreamSnapshot = core.StreamSnapshot
type ConsumerSnapshot = core.ConsumerSnapshot
type StoredMessageSnapshot = core.StoredMessageSnapshot

func DefaultConfig(deviceID string) Config {
	return core.DefaultConfig(deviceID)
}

func MQTTPublishUpload(ctx context.Context, cfg Config, payload []byte) (*MQTTPublishResult, error) {
	return core.MQTTPublishUpload(ctx, cfg, payload)
}

func MQTTSubscribeCommands(ctx context.Context, cfg Config, handler func(MQTTCommand) error) error {
	return core.MQTTSubscribeCommands(ctx, cfg, handler)
}

func NATSSubscribeUploads(ctx context.Context, cfg Config, handler func(NATSUpload) error) error {
	return core.NATSSubscribeUploads(ctx, cfg, handler)
}

func NATSPublishCommand(ctx context.Context, cfg Config, payload []byte) (*NATSPublishResult, error) {
	return core.NATSPublishCommand(ctx, cfg, payload)
}

func DiagnoseMQTTStream(ctx context.Context, cfg Config, latest int) (*Diagnostic, error) {
	return core.DiagnoseMQTTStream(ctx, cfg, latest)
}
