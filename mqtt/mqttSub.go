package mqtt

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/raindrop1986/nats-gateway/gateway"
	"log"
	"os"
	"os/signal"
	"strings"
)

const (
	mqttBroker   = "tcp://192.168.1.192:1883"
	mqttUser     = "mqtt_only_user"
	mqttPassword = "mqtt@2026"
	deviceID     = "WT260605135206"
)

func MqttSub() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	cfg := sampleConfig()
	log.Printf("mqtt subscribing client_id=%s topic=%s qos=%d", cfg.CommandClientID, cfg.CommandTopic(), cfg.MQTTQoS)
	err := gateway.MQTTSubscribeCommands(ctx, cfg, func(msg gateway.MQTTCommand) error {
		fmt.Printf("mqtt received qos=%d retained=%v topic=%s payload=%s\n",
			msg.QoS, msg.Retained, msg.Topic, strings.ToUpper(hex.EncodeToString(msg.Payload)))
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("mqtt subscribe failed: %v", err)
	}
	log.Println("mqtt subscriber exiting")
}

func sampleConfig() gateway.Config {
	cfg := gateway.DefaultConfig(deviceID)
	cfg.MQTTBroker = mqttBroker
	cfg.MQTTUser = mqttUser
	cfg.MQTTPassword = mqttPassword
	cfg.CommandClientID = deviceID
	cfg.UploadClientID = deviceID + "_upload_simulator"
	return cfg
}
