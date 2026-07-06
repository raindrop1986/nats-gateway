package main

import (
	"fmt"
	mqtt2 "github.com/raindrop1986/nats-gateway/cmd/mqtt"
	"github.com/raindrop1986/nats-gateway/cmd/nats"
	"os"
)

func main() {
	mode := "nats-diag"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	switch mode {
	case "mqtt-pub":
		mqtt2.MqttPub()
	case "mqtt-pub-qos0":
		mqtt2.MqttPubWithQoS(0)
	case "mqtt-sub":
		mqtt2.MqttSub()
	case "nats-sub":
		nats.NatsSubPub("sub")
	case "nats-live-sub":
		nats.NatsSubPub("live-sub")
	case "nats-pub":
		nats.NatsSubPub("pub")
	case "nats-diag":
		nats.NatsSubPub("diag")
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", mode)
		fmt.Fprintln(os.Stderr, "modes: mqtt-pub, mqtt-pub-qos0, mqtt-sub, nats-sub, nats-live-sub, nats-pub, nats-diag")
		os.Exit(2)
	}
}
