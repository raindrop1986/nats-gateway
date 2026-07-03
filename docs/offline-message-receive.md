# 生产模拟：MQTT 设备上行与 NATS 平台下行

当前项目模拟的是生产中的双向通信模型：

```text
设备上行：终端 -> MQTT -> NATS 后端
Topic:   WT1_receive/<设备号>

平台下行：NATS 平台 -> MQTT 网关 -> 终端
Topic:   WT1_service_parameter/<设备号>
```

以当前设备号 `WT260605135206` 为例：

```text
上行 topic: WT1_receive/WT260605135206
下行 topic: WT1_service_parameter/WT260605135206
```

这两个 topic 是不同方向的业务通道，不应该拿 `mqtt.MqttPub()` 直接测试 `mqtt.MqttSub()` 是否能收到；一个是设备上报，一个是平台下发。

## 当前代码角色

| 文件 | 角色 | 当前 topic |
| --- | --- | --- |
| `mqtt/mqtt.go` | 设备上行模拟器，通过 MQTT 发布设备数据 | `WT1_receive/WT260605135206` |
| `mqtt/mqttSub.go` | 设备下行模拟器，通过 MQTT 订阅平台指令 | `WT1_service_parameter/WT260605135206` |
| `nats/natsMq.go` 的 `sub` 模式 | 平台后端，接收设备上行数据 | `$MQTT.msgs.WT1_receive.>` |
| `nats/natsMq.go` 的 `pub` 模式 | 平台后端，下发指令给 MQTT 离线设备 | `$MQTT.msgs.WT1_service_parameter.WT260605135206` |

## MQTT 与 NATS subject 映射

NATS MQTT 网关内部用 NATS subject 表示 MQTT topic。常见映射如下：

```text
MQTT topic/filter                         NATS subject/filter
WT1_receive/WT260605135206                WT1_receive.WT260605135206
WT1_receive/#                             WT1_receive.>
WT1_service_parameter/WT260605135206      WT1_service_parameter.WT260605135206
```

项目中给 MQTT 离线会话写消息时，还会加上网关内部前缀 `$MQTT.msgs.`：

```text
MQTT 下行 topic:
WT1_service_parameter/WT260605135206

写入 JetStream 的 subject:
$MQTT.msgs.WT1_service_parameter.WT260605135206
```

注意不要写成下面这种带 `/` 的形式：

```text
$MQTT.msgs.WT1_service_parameter/WT260605135206
```

`/` 是 MQTT topic 分隔符；NATS subject 里应该用 `.`。

## 上行链路：设备通过 MQTT 上传，平台通过 NATS 接收

1. 平台后端启动 NATS durable 订阅：

   ```go
   nats.NatsSubPub("sub")
   ```

2. 代码会订阅：

   ```text
   $MQTT.msgs.WT1_receive.>
   ```

3. 设备模拟器通过 MQTT 发布：

   ```go
   mqtt.MqttPub()
   ```

4. MQTT topic 是：

   ```text
   WT1_receive/WT260605135206
   ```

5. NATS 后端收到时会打印为：

   ```text
   mqtt_topic=WT1_receive/WT260605135206
   ```

这条链路用于模拟“终端上报数据，平台后端接收”。如果后端程序离线，JetStream durable consumer 会保存消费进度，后端重启后继续从上次 ack 后的位置消费。

## 下行链路：平台通过 NATS 下发，离线 MQTT 设备重连接收

1. 设备先上线订阅平台指令：

   ```go
   mqtt.MqttSub()
   ```

2. MQTT 订阅端使用固定 ClientID：

   ```go
   commandSubClientID = "WT260605135206"
   ```

3. MQTT 订阅端订阅：

   ```text
   WT1_service_parameter/WT260605135206
   ```

4. 订阅成功后停止 `mqtt.MqttSub()`，模拟设备离线。

5. 平台通过 NATS 下发指令：

   ```go
   nats.NatsSubPub("pub")
   ```

6. 代码实际写入的 JetStream subject 是：

   ```text
   $MQTT.msgs.WT1_service_parameter.WT260605135206
   ```

7. 消息 header 设置：

   ```go
   Nmqtt-Pub: 1
   ```

8. 再启动 `mqtt.MqttSub()`，使用相同 ClientID 重连。服务端找到原来的 MQTT 持久会话后，会把离线期间的 QoS1 下行消息继续投递给该设备。

这条链路用于模拟“平台下发命令，设备当时离线，设备重连后收到命令”。

## 为什么必须固定 ClientID

MQTT 离线消息绑定的是 MQTT 会话，而会话由 ClientID 标识。

当前下行订阅端使用：

```go
SetClientID("WT260605135206")
SetCleanSession(false)
```

含义：

- `SetClientID("WT260605135206")`：每次重连都回到同一个设备会话。
- `SetCleanSession(false)`：断开后服务端保留订阅关系和未投递的 QoS1/QoS2 消息。
- 如果换 ClientID，服务端会认为这是另一个设备会话，之前积压的离线消息不会投递给它。

因为当前模拟器把“设备上行发布”和“设备下行订阅”拆成两个 Go 入口，`mqtt.MqttPub()` 使用了单独的测试 ClientID：

```go
uploadPubClientID = "WT260605135206_upload_simulator"
```

这样可以避免两个模拟程序同时在线时使用相同 ClientID 互相踢下线。真实设备通常会用同一条 MQTT 连接同时 publish 上行数据并 subscribe 下行指令。

## NATS 下发为什么不能直接发布普通 subject

如果平台只是普通发布：

```text
WT1_service_parameter.WT260605135206
```

在线 MQTT 客户端可能能收到实时消息，但这不是当前项目用来验证离线下发的路径。要让离线 MQTT 持久会话重连后收到，需要写入 MQTT 网关的 JetStream 消息 subject：

```text
$MQTT.msgs.WT1_service_parameter.WT260605135206
```

并设置 MQTT QoS header：

```text
Nmqtt-Pub: 1
Nmqtt-Subject: WT1_service_parameter.WT260605135206
Nmqtt-Mapped: WT1_service_parameter.WT260605135206
```

这也是 `nats.NatsSubPub("pub")` 当前做的事。

NATS 官方文档说明：普通 NATS 消息投递到 MQTT 订阅时会按 QoS0 处理。因此，普通 NATS subject 发布只能作为在线实时投递验证，不能作为 MQTT 离线 QoS1 投递验证。

当前代码写 `$MQTT.msgs.>` 属于使用 NATS Server 的 MQTT 网关内部持久化 subject。它可以用来排查和模拟，但生产使用前需要固定 NATS Server 版本并做回归测试，因为这不是普通业务 subject。

## 离线下发排查流程

现在 `main.go` 支持通过参数选择模式：

```text
go run . mqtt-sub
go run . mqtt-pub
go run . nats-sub
go run . nats-pub
go run . nats-diag
```

建议按下面顺序排查下行离线：

1. 启动设备订阅，确认会话创建成功。

   ```text
   go run . mqtt-sub
   ```

   日志里要看到：

   ```text
   mqtt subscribed client_id=WT260605135206 topic=WT1_service_parameter/WT260605135206 qos=1
   ```

2. 停掉 `mqtt-sub`，模拟设备离线。

3. 运行诊断：

   ```text
   go run . nats-diag
   ```

   期望至少看到 `$MQTT_msgs` 上存在 MQTT 会话 consumer。若输出 `no consumer found`，说明 MQTT QoS1 持久订阅并没有在服务端建起来，通常是 ClientID、CleanSession、权限或服务端 JetStream 账号问题。

4. 平台下发：

   ```text
   go run . nats-pub
   ```

5. 再运行诊断：

   ```text
   go run . nats-diag
   ```

   期望最近消息类似：

   ```text
   subject=$MQTT.msgs.WT1_service_parameter.WT260605135206
   qos=1
   mqtt_subject=WT1_service_parameter.WT260605135206
   mqtt_mapped=WT1_service_parameter.WT260605135206
   ```

6. 重新启动设备：

   ```text
   go run . mqtt-sub
   ```

   如果前面 consumer 存在、消息也进入 `$MQTT_msgs`，但这里仍收不到，就重点看 NATS Server 日志里是否有 MQTT consumer 投递、权限拒绝或 session collision。

排查下行离线时，一般不需要同时运行 `go run . nats-sub`。当前代码里的 `nats-sub` 只过滤 `$MQTT.msgs.WT1_receive.>`，不会消费 `WT1_service_parameter` 下行消息；先关闭它可以让日志更干净，避免把上行链路和下行链路混在一起看。

## 配置注意点

你贴出的 NATS Server 配置整体方向是对的：

- `jetstream` 必须开启，否则 MQTT QoS1/QoS2 和持久会话没有持久化基础。
- MQTT 账号需要 `allowed_connection_types: ["MQTT"]`。
- 后端 NATS 账号需要 `allowed_connection_types: ["STANDARD"]`。
- 后端 NATS 账号需要能 publish `$MQTT.msgs.>`，否则不能通过 NATS 写入 MQTT 离线消息。
- 后端 NATS 账号需要能访问 `$JS.API.>`、`$JS.ACK.>`、`_INBOX.>`，否则 JetStream durable consumer 和 ack 会失败。

如果以后把权限从 `>` 收紧，记得权限里写 NATS subject 形式：

```text
WT1_receive.>
WT1_service_parameter.WT260605135206
$MQTT.msgs.WT1_receive.>
$MQTT.msgs.WT1_service_parameter.>
```

不要把 MQTT 的 `/` 分隔符直接写进 NATS subject 权限。
