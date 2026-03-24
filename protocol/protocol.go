package protocol

import "encoding/json"

const (
	TypeRequestMatch = 1
	TypeCancelMatch  = 2
	TypeLeaveRoom    = 3
	TypeDissolveRoom = 4
	TypeStatus       = 5
	TypeRoomWaiting  = 6
	TypeError        = "error"
)

type ClientMessage struct {
	Type int `json:"type"`
	Num  int `json:"num,omitempty"`
}

type ServerMessage struct {
	Type         interface{} `json:"type"`
	Num          int         `json:"num,omitempty"`
	PlayerCount  int         `json:"player_count,omitempty"`  // 本局匹配到的人数（2~4），专服/UI 应用此值，勿把 Godot 的 server peer id=1 算成玩家
	MaxPlayers   int         `json:"max_players,omitempty"`   // 房间最大人数（当前固定 4）
	WaitSeconds  int         `json:"wait_seconds,omitempty"`  // 房间待开局剩余秒数（>=0）
	RoomID       string      `json:"room_id,omitempty"`       // 匹配服房间号（等待阶段可用于日志）
	RoomReady    *bool       `json:"room_ready,omitempty"`    // true 表示已满足开局条件
	OK           *bool       `json:"ok,omitempty"`
	Reason       string      `json:"reason,omitempty"`
	Online       int         `json:"online,omitempty"`
	Matching     int         `json:"matching,omitempty"`
	InGame       int         `json:"in_game,omitempty"`
}

func ParseClientMessage(data []byte) (*ClientMessage, error) {
	var msg ClientMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

func MarshalServerMessage(msg *ServerMessage) ([]byte, error) {
	return json.Marshal(msg)
}

func MatchSuccessMsg(port, playerCount int) []byte {
	msg := ServerMessage{Type: TypeRequestMatch, Num: port, PlayerCount: playerCount}
	b, _ := json.Marshal(msg)
	return b
}

func RoomWaitingMsg(roomID string, playerCount, maxPlayers, waitSeconds int, ready bool) []byte {
	msg := ServerMessage{
		Type:        TypeRoomWaiting,
		RoomID:      roomID,
		PlayerCount: playerCount,
		MaxPlayers:  maxPlayers,
		WaitSeconds: waitSeconds,
		RoomReady:   &ready,
	}
	b, _ := json.Marshal(msg)
	return b
}

func CancelMatchOKMsg() []byte {
	ok := true
	msg := ServerMessage{Type: TypeCancelMatch, OK: &ok}
	b, _ := json.Marshal(msg)
	return b
}

func RoomDissolvedMsg(reason string) []byte {
	msg := ServerMessage{Type: TypeDissolveRoom, Reason: reason}
	b, _ := json.Marshal(msg)
	return b
}

func ErrorMsg(reason string) []byte {
	msg := ServerMessage{Type: TypeError, Reason: reason}
	b, _ := json.Marshal(msg)
	return b
}

func StatusMsg(online, matching, inGame int) []byte {
	msg := ServerMessage{Type: TypeStatus, Online: online, Matching: matching, InGame: inGame}
	b, _ := json.Marshal(msg)
	return b
}

/*
匹配服 WebSocket 接口文档
========================
连接地址: ws://127.0.0.1:8765/ws
传输格式: JSON 文本帧（仅接受 TextMessage，不接受 Binary）
心跳机制: 服务端每 30s 发送 Ping，客户端需回复 Pong；60s 无响应断开连接

匹配规则: 优先凑满 4 人开房，不够则 3 人，最少 2 人即可开房
房间人数: 2 ~ 4 人，第一个入队的玩家为房主

═══════════════════════════════════════════════════════════════
客户端 → 服务端
═══════════════════════════════════════════════════════════════

1. 请求匹配  {"type": 1}
   - 加入匹配队列，等待配对
   - 重复发送不会重复入队
   - 已在房间中发送会收到错误

2. 取消匹配  {"type": 2}
   - 从匹配队列中移除

3. 退出房间  {"type": 3}
   - 玩家主动离开当前房间（匹配服上不再计为「房内」）
   - 专服进程：**仅当房内已无任何仍在连接且处于进房状态的玩家时**才关闭并回收端口；否则专服继续运行，其余玩家**不会**因此收到 type=4 解散推送
   - WebSocket 断开视同逐步退房：同样按「最后一人离开」才回收专服

4. 解散房间  {"type": 4}
   - 仅房主可操作
   - 房间立即解散，所有其他玩家收到 type=4 通知，专服进程关闭

═══════════════════════════════════════════════════════════════
服务端 → 客户端（主动推送）
═══════════════════════════════════════════════════════════════

1. 匹配成功        {"type": 1, "num": 18000, "player_count": 2}
   - num = 专服端口号，客户端拿此端口连接 dedicated server
   - player_count = 本局实际匹配人数（2~4）。UI/专服只应显示这么多「真人玩家位」；Godot 专服上 multiplayer 的 **peer id=1 多为服务端权威**，不要与真人混在一个列表里当成第 3 人

2. 取消匹配确认    {"type": 2, "ok": true}

3. 房间解散通知    {"type": 4, "reason": "..."}
   - reason 取值：
     "房主解散房间"  — 房主主动解散(type=4)
   - 说明：非房主发送 type=3 或他人断线**不再**向其余玩家推送解散（专服可能仍在运行）；仅「最后一人离开」时服端回收专服，通常不再向已无人连接端推送

4. 状态广播        {"type": 5, "online": 10, "matching": 3, "in_game": 2}
   - 在以下时机自动推送给所有已连接的客户端：
     · 有新客户端连接 / 断开
     · 有玩家加入 / 退出匹配队列
     · 匹配成功（队列人数变化）
     · 房间解散
   - online   = 当前总在线连接数
   - matching = 当前处于匹配阶段的人数（含队列 + 等待房）
   - in_game  = 当前已进入游戏中的人数（已分配专服并进入房内）

5. 错误消息        {"type": "error", "reason": "..."}
   - reason 取值：
     "already in a room"              — 已在房间中却请求匹配
     "not in a room"                  — 不在房间中却请求退出/解散
     "only owner can dissolve room"   — 非房主尝试解散
     "invalid JSON"                   — 消息格式错误
     "only text frames accepted"      — 发送了非文本帧
     "unknown type: N"                — 未知的 type 值

═══════════════════════════════════════════════════════════════
客户端状态机（建议）
═══════════════════════════════════════════════════════════════

  [空闲] --发送type=1--> [匹配中] --收到type=1--> [房间内]
  [匹配中] --发送type=2--> [空闲]
  [房间内] --发送type=3--> [仍可对局或空闲]（仅当你是最后一人离开匹配服视角的房间时专服才停）
  [房间内] --发送type=4(房主)--> [空闲]
  [房间内] --收到type=4--> [空闲]
  [任意状态] --随时可能收到 type=5 状态广播
  [任意状态] --断线--> 服务端自动清理

═══════════════════════════════════════════════════════════════
Godot 客户端接入要点
═══════════════════════════════════════════════════════════════

  1. 使用 WebSocketPeer 连接 ws://127.0.0.1:8765/ws
  2. 发送/接收均为 JSON 字符串（put_packet / get_packet → UTF-8）
  3. 收到 type=1 后，用 num 连接专服；用 player_count 限制 UI 槽位数（避免把 server id=1 算成未连接玩家）
  4. 收到 type=5 后，用 online / matching / in_game 更新 UI 显示
  5. 必须正确响应 Ping/Pong（Godot WebSocketPeer 默认支持）
  6. 断线重连后需重新发送 type=1 请求匹配（服务端不保留会话）
*/