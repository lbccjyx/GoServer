package matchmaker

import (
	"log"

	"GoServer/protocol"
)

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
	if c.State == StateWaiting && c.RoomID != "" {
		_ = m.cancelFromWaitingRoomLocked(id)
		delete(m.clients, id)
		log.Printf("[matchmaker] client %s ws closed (was waiting in room %s)", id, c.RoomID)
		m.tryMatchLocked()
		m.broadcastStatusLocked()
		return
	}
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

func safeSend(ch chan []byte, data []byte) {
	defer func() { recover() }()
	select {
	case ch <- data:
	default:
	}
}

// broadcastStatusLocked 向所有已连接的客户端广播在线/匹配中/游戏中人数
func (m *Matchmaker) broadcastStatusLocked() {
	online := len(m.clients)
	matching := 0
	inGame := 0
	for _, c := range m.clients {
		if c == nil {
			continue
		}
		if c.State == StateWaiting {
			matching++
		} else if c.State == StateInRoom {
			inGame++
		}
	}
	msg := protocol.StatusMsg(online, matching, inGame)
	for _, c := range m.clients {
		safeSend(c.Send, msg)
	}
}
