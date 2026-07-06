# Gateway 库使用说明

核心库在 `gateway` 包中，其他项目可以直接引用：

```go
import "github.com/raindrop1986/nats-gateway"
```

如果是在另一个本地项目中调试，可以在对方项目的 `go.mod` 中添加本地替换：

```text
require github.com/raindrop1986/nats-gateway v0.0.0

replace github.com/raindrop1986/nats-gateway => E:\golang_study\nats
```

## 四个核心能力

| 生产角色 | 函数 | 说明 |
| --- | --- | --- |
| 终端 MQTT 上行 | `gateway.DeviceUpload` | 设备上报 `WT1_receive/<设备号>`。 |
| 终端 MQTT 下行 | `gateway.DeviceReceiveCommands` | 设备持久订阅 `WT1_service_parameter/<设备号>`。 |
| 平台 NATS 接 QoS1/QoS2 上行 | `gateway.PlatformReceiveUploads` | 平台接收可持久化的设备上报消息。 |
| 平台 NATS 接 QoS0 在线上行 | `gateway.PlatformReceiveLiveUploads` | 平台接收在线实时设备上报消息，不做离线补收。 |
| 平台 NATS 下发 | `gateway.PlatformSendCommand` | 平台下发指令，使离线 MQTT 设备重连后收到。 |

另有诊断函数：

```go
gateway.DiagnoseMQTTStream(ctx, cfg, 5)
```

用于查看 `$MQTT_msgs`、consumer 和最近消息 header。

## 基础配置

```go
cfg := gateway.DefaultConfig("WT260605135206")

cfg.MQTTBroker = "tcp://192.168.1.192:1883"
cfg.MQTTUser = "mqtt_only_user"
cfg.MQTTPassword = "mqtt@2026"

cfg.NATSURL = "nats://192.168.1.192:4222"
cfg.NATSUser = "nats_backend"
cfg.NATSPassword = "nats@2026"
```

默认 topic：

```text
上行: WT1_receive/WT260605135206
下行: WT1_service_parameter/WT260605135206
```

如果生产 topic 前缀不同，可以覆盖：

```go
cfg.UploadTopicPrefix = "WT1_receive"
cfg.CommandTopicPrefix = "WT1_service_parameter"
cfg.UploadFilter = "WT1_receive/#"
```

## 终端 MQTT 上行

```go
result, err := gateway.DeviceUpload(ctx, cfg, payload)
if err != nil {
    return err
}
fmt.Println(result.Topic, result.Bytes)
```

## 终端 MQTT 下行

```go
err := gateway.DeviceReceiveCommands(ctx, cfg, func(msg gateway.MQTTCommand) error {
    fmt.Printf("topic=%s payload=%x\n", msg.Topic, msg.Payload)
    return nil
})
```

这个函数会阻塞直到 `ctx` 取消或回调返回错误。离线消息能否收到取决于：

- `cfg.CommandClientID` 固定不变，默认等于 `DeviceID`。
- MQTT 使用 `CleanSession(false)`。
- 订阅 QoS 默认为 1。

## 平台 NATS 接收上行

### QoS1/QoS2 可持久化接收

```go
err := gateway.PlatformReceiveUploads(ctx, cfg, func(msg gateway.NATSUpload) error {
    fmt.Printf("topic=%s payload=%x\n", msg.Topic, msg.Payload)
    return nil
})
```

该函数会创建/绑定 durable consumer，默认 durable 名称是：

```text
backend_mqtt_consumer
```

### QoS0 在线实时接收

QoS0 不进入 `$MQTT_msgs` 持久化流，不能离线补收。要接收 QoS0 上报，请使用普通 NATS 在线订阅：

```go
err := gateway.PlatformReceiveLiveUploads(ctx, cfg, func(msg gateway.NATSUpload) error {
    fmt.Printf("topic=%s payload=%x\n", msg.Topic, msg.Payload)
    return nil
})
```

它订阅的是普通 NATS subject，例如：

```text
WT1_receive.>
```

这类消息只有订阅端在线时才能收到。

## 平台 NATS 下发指令

```go
result, err := gateway.PlatformSendCommand(ctx, cfg, payload)
if err != nil {
    return err
}
fmt.Println(result.Topic, result.Sequence)
```

内部会写入：

```text
$MQTT.msgs.WT1_service_parameter.WT260605135206
```

并带上 MQTT QoS header：

```text
Nmqtt-Pub: 1
Nmqtt-Subject: WT1_service_parameter.WT260605135206
Nmqtt-Mapped: WT1_service_parameter.WT260605135206
```

## 当前示例入口

项目根目录仍保留了可运行示例：

```text
go run ./cmd/nats-gateway mqtt-pub
go run ./cmd/nats-gateway mqtt-pub-qos0
go run ./cmd/nats-gateway mqtt-sub
go run ./cmd/nats-gateway nats-sub
go run ./cmd/nats-gateway nats-live-sub
go run ./cmd/nats-gateway nats-pub
go run ./cmd/nats-gateway nats-diag
```

这些示例入口现在只是 `gateway` 包的薄包装，方便本项目继续验证。
