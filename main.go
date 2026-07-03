package main

import (
	"fmt"
	"nast-gateway/mqtt"
	"nast-gateway/nats"
	"os"
)

func main() {
	mode := "nats-diag"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	switch mode {
	case "mqtt-pub":
		mqtt.MqttPub()
	case "mqtt-sub":
		mqtt.MqttSub()
	case "nats-sub":
		nats.NatsSubPub("sub")
	case "nats-pub":
		nats.NatsSubPub("pub")
	case "nats-diag":
		nats.NatsSubPub("diag")
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", mode)
		fmt.Fprintln(os.Stderr, "modes: mqtt-pub, mqtt-sub, nats-sub, nats-pub, nats-diag")
		os.Exit(2)
	}
}
