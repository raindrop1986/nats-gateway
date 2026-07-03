# Gateway 库使用说明

核心库在 `gateway` 包中，其他项目可以直接引用：

```go
import "github.com/raindrop1986/nats-gateway"
```

如果是在另一个本地项目中调试，可以在对方项目的 `go.mod` 中添加本地替换：

```text
require nast-gateway v0.0.0

replace nast-gateway => E:\golang_study\nats
```

## 四个核心能力

| 生产角色 | 函数 | 说明 |
| --- | --- | --- |
| 终端 MQTT 上行 | `gateway.MQTTPublishUpload` | 终端通过 MQTT 发布 `WT1_receive/<设备号>`。 |
| 终端 MQTT 下行 | `gateway.MQTTSubscribeCommands` | 终端通过固定 ClientID + QoS1 持久订阅 `WT1_service_parameter/<设备号>`。 |
| 平台 NATS 接上行 | `gateway.NATSSubscribeUploads` | 平台通过 NATS durable consumer 接收 MQTT 网关中的上行消息。 |
| 平台 NATS 下发 | `gateway.NATSPublishCommand` | 平台通过 NATS 写入 MQTT 网关持久化 Stream，使离线 MQTT 终端重连后收到指令。 |

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
result, err := gateway.MQTTPublishUpload(ctx, cfg, payload)
if err != nil {
    return err
}
fmt.Println(result.Topic, result.Bytes)
```

## 终端 MQTT 下行

```go
err := gateway.MQTTSubscribeCommands(ctx, cfg, func(msg gateway.MQTTCommand) error {
    fmt.Printf("topic=%s payload=%x\n", msg.Topic, msg.Payload)
    return nil
})
```

这个函数会阻塞直到 `ctx` 取消或回调返回错误。离线消息能否收到取决于：

- `cfg.CommandClientID` 固定不变，默认等于 `DeviceID`。
- MQTT 使用 `CleanSession(false)`。
- 订阅 QoS 默认为 1。

## 平台 NATS 接收上行

```go
err := gateway.NATSSubscribeUploads(ctx, cfg, func(msg gateway.NATSUpload) error {
    fmt.Printf("topic=%s payload=%x\n", msg.Topic, msg.Payload)
    return nil
})
```

该函数会创建/绑定 durable consumer，默认 durable 名称是：

```text
backend_mqtt_consumer
```

## 平台 NATS 下发指令

```go
result, err := gateway.NATSPublishCommand(ctx, cfg, payload)
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
go run . mqtt-pub
go run . mqtt-sub
go run . nats-sub
go run . nats-pub
go run . nats-diag
```

这些示例入口现在只是 `gateway` 包的薄包装，方便本项目继续验证。
