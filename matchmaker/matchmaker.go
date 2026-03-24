package matchmaker

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"GoServer/protocol"
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

type Room struct {
	ID        string
	OwnerID   string
	PlayerIDs []string
	Port      int
	CreatedAt time.Time
	Process   *exec.Cmd
}

type Matchmaker struct {
	mu        sync.Mutex
	clients   map[string]*Client
	rooms     map[string]*Room
	queue     []string
	queueSet  map[string]bool
	nextPort  int
	freePorts []int
}

func New() *Matchmaker {
	return &Matchmaker{
		clients:   make(map[string]*Client),
		rooms:     make(map[string]*Room),
		queue:     make([]string, 0),
		queueSet:  make(map[string]bool),
		nextPort:  18000,
		freePorts: make([]int, 0),
	}
}

func (m *Matchmaker) Register(id string, sendCh chan []byte) *Client {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := &Client{ID: id, Send: sendCh, State: StateIdle}
	m.clients[id] = c
	log.Printf("[matchmaker] client registered: %s", id)
	m.broadcastStatusLocked()
	return c
}

func (m *Matchmaker) Unregister(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeFromQueueLocked(id)
	c, ok := m.clients[id]
	if !ok {
		return
	}
	// 已匹配进房后：不在「第一个断线」时拆房；仅当房间内已无任何仍在匹配服上登记为 StateInRoom 的玩家时回收专服。
	if c.State == StateInRoom {
		roomID := c.RoomID
		delete(m.clients, id)
		log.Printf("[matchmaker] client %s ws closed (was in room %s)", id, roomID)
		if room, ok := m.rooms[roomID]; ok && m.roomLiveMatchmakerPeersLocked(room) == 0 {
			log.Printf("[matchmaker] room %s: no matchmaker clients left (ws), recycling dedicated server", roomID)
			m.recycleRoomLocked(room)
		}
		m.broadcastStatusLocked()
		return
	}
	delete(m.clients, id)
	log.Printf("[matchmaker] client unregistered: %s", id)
	m.broadcastStatusLocked()
}

func (m *Matchmaker) RequestMatch(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	c, ok := m.clients[id]
	if !ok {
		return
	}
	if c.State == StateInRoom {
		safeSend(c.Send, protocol.ErrorMsg("already in a room"))
		return
	}

	if m.queueSet[id] {
		log.Printf("[matchmaker] client %s already in queue, skip", id)
		return
	}

	c.State = StateWaiting
	m.queue = append(m.queue, id)
	m.queueSet[id] = true
	log.Printf("[matchmaker] client %s enqueued, queue size=%d", id, len(m.queue))

	m.tryMatchLocked()
	m.broadcastStatusLocked()
}

func (m *Matchmaker) CancelMatch(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.removeFromQueueLocked(id)
	if c, ok := m.clients[id]; ok && c.State == StateWaiting {
		c.State = StateIdle
	}
	if c, ok := m.clients[id]; ok {
		safeSend(c.Send, protocol.CancelMatchOKMsg())
	}
	log.Printf("[matchmaker] client %s cancelled match", id)
	m.broadcastStatusLocked()
}

func (m *Matchmaker) LeaveRoom(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handleLeaveLocked(id)
	m.broadcastStatusLocked()
}

func (m *Matchmaker) DissolveRoom(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	c, ok := m.clients[id]
	if !ok || c.State != StateInRoom {
		if ok {
			safeSend(c.Send, protocol.ErrorMsg("not in a room"))
		}
		return
	}

	room, ok := m.rooms[c.RoomID]
	if !ok {
		return
	}

	if room.OwnerID != id {
		safeSend(c.Send, protocol.ErrorMsg("only owner can dissolve room"))
		return
	}

	log.Printf("[matchmaker] owner %s dissolves room %s", id, room.ID)
	dissolveMsg := protocol.RoomDissolvedMsg("房主解散房间")
	for _, pid := range room.PlayerIDs {
		if pid == id {
			continue
		}
		if pc, ok := m.clients[pid]; ok {
			safeSend(pc.Send, dissolveMsg)
			pc.State = StateIdle
			pc.RoomID = ""
		}
	}

	c.State = StateIdle
	c.RoomID = ""
	m.recycleRoomLocked(room)
	m.broadcastStatusLocked()
}

// --- internal ---

// tryMatchLocked 优先凑 4 人，不够则 3 人，再不够则 2 人
func (m *Matchmaker) tryMatchLocked() {
	for {
		qLen := m.validQueueLenLocked()
		if qLen < 2 {
			break
		}

		var take int
		switch {
		case qLen >= 4:
			take = 4
		case qLen >= 3:
			take = 3
		default:
			take = 2
		}

		ids := m.dequeueNLocked(take)
		if len(ids) < 2 {
			break
		}

		port := m.allocPortLocked()
		roomID := fmt.Sprintf("room-%d", port)

		room := &Room{
			ID:        roomID,
			OwnerID:   ids[0],
			PlayerIDs: ids,
			Port:      port,
			CreatedAt: time.Now(),
		}
		m.rooms[roomID] = room

		matchMsg := protocol.MatchSuccessMsg(port, len(ids))
		for _, pid := range ids {
			if c, ok := m.clients[pid]; ok {
				c.State = StateInRoom
				c.RoomID = roomID
				safeSend(c.Send, matchMsg)
			}
		}

		go m.startDedicatedServer(room)

		log.Printf("[matchmaker] matched %v -> room=%s port=%d (%d players)", ids, roomID, port, len(ids))
	}
}

// validQueueLenLocked 统计队列中仍处于 Waiting 状态的有效人数
func (m *Matchmaker) validQueueLenLocked() int {
	count := 0
	for _, id := range m.queue {
		if c, ok := m.clients[id]; ok && c.State == StateWaiting {
			count++
		}
	}
	return count
}

// dequeueNLocked 从队头弹出最多 n 个有效（StateWaiting）的客户端 ID
func (m *Matchmaker) dequeueNLocked(n int) []string {
	result := make([]string, 0, n)
	newQueue := make([]string, 0, len(m.queue))

	for _, id := range m.queue {
		if len(result) < n {
			if c, ok := m.clients[id]; ok && c.State == StateWaiting {
				result = append(result, id)
				delete(m.queueSet, id)
				continue
			}
		}
		newQueue = append(newQueue, id)
	}
	m.queue = newQueue
	return result
}

// resolveDedicatedServerExe 返回专服可执行文件的绝对路径。
// Go 1.19+ 在 Windows 上禁止用「仅文件名 / 相对当前目录」启动程序，必须用绝对路径。
// 解析顺序：环境变量 DEDICATED_SERVER → 与匹配服同目录的 DedicatedServer.exe → 当前工作目录下的 DedicatedServer.exe
func resolveDedicatedServerExe() (string, error) {
	const defaultName = "DedicatedServer.exe"

	if p := os.Getenv("DEDICATED_SERVER"); p != "" {
		return filepath.Abs(filepath.Clean(p))
	}

	if self, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(self), defaultName)
		if _, err := os.Stat(candidate); err == nil {
			return filepath.Abs(candidate)
		}
	}

	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Abs(filepath.Join(wd, defaultName))
}

// resolveDedicatedServerLogDir 专服 stdout/stderr 日志目录。
// 优先环境变量 DEDICATED_SERVER_LOG_DIR；否则为「匹配服 exe 所在目录/logs」；再否则为「当前工作目录/logs」。
func resolveDedicatedServerLogDir() (string, error) {
	if d := os.Getenv("DEDICATED_SERVER_LOG_DIR"); d != "" {
		return filepath.Abs(filepath.Clean(d))
	}
	if self, err := os.Executable(); err == nil {
		return filepath.Abs(filepath.Join(filepath.Dir(self), "logs"))
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Abs(filepath.Join(wd, "logs"))
}

func (m *Matchmaker) startDedicatedServer(room *Room) {
	exePath, err := resolveDedicatedServerExe()
	if err != nil {
		log.Printf("[matchmaker] resolve dedicated server path: %v", err)
		return
	}
	if _, err := os.Stat(exePath); err != nil {
		log.Printf("[matchmaker] dedicated server not found: %q (%v). 设置环境变量 DEDICATED_SERVER 为完整路径，或与匹配服 exe 同目录放置 DedicatedServer.exe", exePath, err)
		return
	}

	logDir, err := resolveDedicatedServerLogDir()
	if err != nil {
		log.Printf("[matchmaker] dedicated server log dir: %v", err)
		logDir = "."
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		log.Printf("[matchmaker] mkdir log dir %q: %v", logDir, err)
	}

	started := time.Now()
	logName := fmt.Sprintf("DedicatedServer_%s_%09d_port%d.log", started.Format("20060102_150405"), started.Nanosecond(), room.Port)
	logPath := filepath.Join(logDir, logName)

	var logFile *os.File
	logFile, err = os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		log.Printf("[matchmaker] open dedicated log %q: %v (专服仍将启动，但日志不落盘)", logPath, err)
		logFile = nil
	} else {
		_, _ = fmt.Fprintf(logFile, "--- DedicatedServer start %s ---\nexe=%s\nargs=--port %d\nroom=%s\n\n",
			started.Format(time.RFC3339Nano), exePath, room.Port, room.ID)
	}

	cmd := exec.Command(exePath, "--port", fmt.Sprintf("%d", room.Port))
	if logFile != nil {
		cmd.Stdout = logFile
		cmd.Stderr = logFile
	}
	if err := cmd.Start(); err != nil {
		if logFile != nil {
			_, _ = fmt.Fprintf(logFile, "\n--- start failed: %v ---\n", err)
			_ = logFile.Close()
		}
		log.Printf("[matchmaker] failed to start dedicated server %q on port %d: %v", exePath, room.Port, err)
		return
	}
	log.Printf("[matchmaker] launching dedicated server: %s --port %d (log=%s)", exePath, room.Port, logPath)
	m.mu.Lock()
	room.Process = cmd
	m.mu.Unlock()
	log.Printf("[matchmaker] dedicated server started on port %d, pid=%d", room.Port, cmd.Process.Pid)

	waitErr := cmd.Wait()
	exited := time.Now()
	if logFile != nil {
		_, _ = fmt.Fprintf(logFile, "\n--- DedicatedServer exited at %s (duration=%s) ---\n", exited.Format(time.RFC3339Nano), exited.Sub(started))
		if waitErr != nil {
			_, _ = fmt.Fprintf(logFile, "wait error: %v\n", waitErr)
		}
		if err := logFile.Close(); err != nil {
			log.Printf("[matchmaker] close dedicated log %q: %v", logPath, err)
		}
	}
	if waitErr != nil {
		log.Printf("[matchmaker] dedicated server on port %d exited: %v", room.Port, waitErr)
	}

	// 专服进程已结束：若房间记录仍在且仍关联本次 cmd，回收端口（勿再 Kill）。
	m.mu.Lock()
	if r, ok := m.rooms[room.ID]; ok && r.Process == cmd {
		r.Process = nil
		m.freePorts = append(m.freePorts, r.Port)
		delete(m.rooms, r.ID)
		log.Printf("[matchmaker] room %s released after dedicated exit, port %d returned to pool (pool size=%d)", r.ID, r.Port, len(m.freePorts))
	}
	m.mu.Unlock()
}

// roomLiveMatchmakerPeersLocked 统计「本房 PlayerIDs 里仍在 m.clients 且 StateInRoom 指向本房」的人数。
// 专服生命周期与此一致：任一人仍在匹配服上保持进房状态则不断专服；最后一人 leave / 断线后再 recycle。
func (m *Matchmaker) roomLiveMatchmakerPeersLocked(room *Room) int {
	if room == nil {
		return 0
	}
	n := 0
	for _, pid := range room.PlayerIDs {
		pc, ok := m.clients[pid]
		if !ok {
			continue
		}
		if pc.State == StateInRoom && pc.RoomID == room.ID {
			n++
		}
	}
	return n
}

func (m *Matchmaker) handleLeaveLocked(id string) {
	c, ok := m.clients[id]
	if !ok || c.State != StateInRoom {
		return
	}

	room, ok := m.rooms[c.RoomID]
	if !ok {
		c.State = StateIdle
		c.RoomID = ""
		return
	}

	c.State = StateIdle
	c.RoomID = ""

	rem := m.roomLiveMatchmakerPeersLocked(room)
	if rem == 0 {
		log.Printf("[matchmaker] room %s: last player left (leave_room), recycling dedicated server", room.ID)
		m.recycleRoomLocked(room)
		return
	}
	log.Printf("[matchmaker] client %s leave_room from %s (%d still on matchmaker)", id, room.ID, rem)
}

func (m *Matchmaker) removeFromQueueLocked(id string) {
	if !m.queueSet[id] {
		return
	}
	delete(m.queueSet, id)
	for i, qid := range m.queue {
		if qid == id {
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			break
		}
	}
}

func (m *Matchmaker) allocPortLocked() int {
	if len(m.freePorts) > 0 {
		port := m.freePorts[len(m.freePorts)-1]
		m.freePorts = m.freePorts[:len(m.freePorts)-1]
		log.Printf("[matchmaker] reusing recycled port %d", port)
		return port
	}
	port := m.nextPort
	m.nextPort++
	return port
}

func (m *Matchmaker) recycleRoomLocked(room *Room) {
	if room.Process != nil && room.Process.Process != nil {
		_ = room.Process.Process.Kill()
		log.Printf("[matchmaker] killed dedicated server on port %d", room.Port)
	}
	room.Process = nil
	// 若专服 goroutine 仍在 Wait，待其结束后会再次尝试 release；此处已 delete 房间则 Wait 尾逻辑 no-op。
	m.freePorts = append(m.freePorts, room.Port)
	delete(m.rooms, room.ID)
	log.Printf("[matchmaker] room %s recycled, port %d returned to pool (pool size=%d)", room.ID, room.Port, len(m.freePorts))
}

// broadcastStatusLocked 向所有已连接的客户端广播当前在线人数和匹配队列人数
func (m *Matchmaker) broadcastStatusLocked() {
	online := len(m.clients)
	matching := len(m.queueSet)
	msg := protocol.StatusMsg(online, matching)
	for _, c := range m.clients {
		safeSend(c.Send, msg)
	}
}

func safeSend(ch chan []byte, data []byte) {
	defer func() { recover() }()
	select {
	case ch <- data:
	default:
	}
}
