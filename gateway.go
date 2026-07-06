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

func DeviceUpload(ctx context.Context, cfg Config, payload []byte) (*MQTTPublishResult, error) {
	return core.DeviceUpload(ctx, cfg, payload)
}

func DeviceReceiveCommands(ctx context.Context, cfg Config, handler func(MQTTCommand) error) error {
	return core.DeviceReceiveCommands(ctx, cfg, handler)
}

func PlatformReceiveUploads(ctx context.Context, cfg Config, handler func(NATSUpload) error) error {
	return core.PlatformReceiveUploads(ctx, cfg, handler)
}

func PlatformReceiveLiveUploads(ctx context.Context, cfg Config, handler func(NATSUpload) error) error {
	return core.PlatformReceiveLiveUploads(ctx, cfg, handler)
}

func PlatformSendCommand(ctx context.Context, cfg Config, payload []byte) (*NATSPublishResult, error) {
	return core.PlatformSendCommand(ctx, cfg, payload)
}

func DiagnoseMQTTStream(ctx context.Context, cfg Config, latest int) (*Diagnostic, error) {
	return core.DiagnoseMQTTStream(ctx, cfg, latest)
}
