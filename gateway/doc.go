// Package gateway provides reusable helpers for a production-style NATS MQTT
// gateway topology:
//
//   - terminal upload through MQTT: WT1_receive/<deviceID>
//   - terminal command subscription through MQTT: WT1_service_parameter/<deviceID>
//   - platform upload consumption through NATS JetStream
//   - platform command publishing through NATS into the MQTT persistent session
package gateway
