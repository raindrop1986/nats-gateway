package mqtt

import (
	"context"
	"encoding/hex"
	"github.com/raindrop1986/nats-gateway/gateway"
	"log"
	"strings"
)

func MqttPub() {
	payload, err := hexPayload()
	if err != nil {
		log.Fatal(err)
	}

	result, err := gateway.MQTTPublishUpload(context.Background(), sampleConfig(), payload)
	if err != nil {
		log.Fatalf("mqtt publish failed: %v", err)
	}
	log.Printf("mqtt published client_id=%s qos=%d topic=%s bytes=%d",
		result.ClientID, result.QoS, result.Topic, result.Bytes)
}

func hexPayload() ([]byte, error) {
	msg := "F5 5A 02 44 0F 4C 09 0C 38 39 38 35 32 30 30 30 32 36 33 33 32 31 34 30 37 37 39 35 56 32 30 32 35 30 36 30 34 56 31 2E 30 2E 30 00 13 00 00 20 CC 01 01 00 D6 B3 00 00 B7 9B DF 02 8D F9 5A 68 EB 58 2F 0B F5 5A 01 14 8D F9 5A 68 A4 8C 70 41 4B 6A C2 42 18 6F 7E 37 AA"
	return hex.DecodeString(strings.ReplaceAll(msg, " ", ""))
}
