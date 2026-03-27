package matchmaker

import (
	"os/exec"
	"sync"
	"time"
)

type ClientState int

const (
	StateIdle    ClientState = 0
	StateWaiting ClientState = 1
	StateInRoom  ClientState = 2
)

type Client struct {
	ID     string
	Send   chan []byte
	State  ClientState
	RoomID string
}

// Room 匹配房间；等待阶段也可能已分配端口并启动专服（第 8 秒预热）。
type Room struct {
	ID                    string
	OwnerID               string
	PlayerIDs             []string
	Port                  int
	MaxPlayers            int
	CreatedAt             time.Time
	WaitUntil             time.Time
	Started               bool
	Process               *exec.Cmd
	dedicatedStartInProgress bool
}

type Matchmaker struct {
	mu         sync.Mutex
	clients    map[string]*Client
	rooms      map[string]*Room
	queue      []string
	queueSet   map[string]bool
	nextRoomID int64
	nextPort   int
	freePorts  []int
}

const (
	roomMinPlayers               = 2
	roomMaxPlayers               = 4
	roomWaitBeforeStart          = 10 * time.Second
	roomDedicatedWarmupBeforeEnd = 2 * time.Second // 倒计时第 8 秒（剩余 2 秒）起专服
	// RoomDissolveTooFewPlayers 等待房仅剩一人或开局后匹配服侧仅剩一人时，被动解散原因文案。
	RoomDissolveTooFewPlayers = "人数不足，房间已关闭"
)

func New() *Matchmaker {
	return &Matchmaker{
		clients:    make(map[string]*Client),
		rooms:      make(map[string]*Room),
		queue:      make([]string, 0),
		queueSet:   make(map[string]bool),
		nextRoomID: 1,
		nextPort:   18000,
		freePorts:  make([]int, 0),
	}
}
