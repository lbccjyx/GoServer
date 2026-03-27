package matchmaker

import (
	"log"
	"time"

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
		if room, ok := m.rooms[roomID]; ok {
			m.dissolveRoomIfSingletonOrEmptyLocked(room)
		}
		m.tryMatchLocked()
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
	rooms := make([]protocol.StatusRoom, 0)
	for _, room := range m.rooms {
		if room == nil || room.Started {
			continue
		}
		if len(room.PlayerIDs) >= room.MaxPlayers {
			continue
		}
		waitSec := int(time.Until(room.WaitUntil).Seconds())
		if waitSec < 0 {
			waitSec = 0
		}
		playerNames := make([]string, 0, len(room.PlayerIDs))
		for _, pid := range room.PlayerIDs {
			pc, ok := m.clients[pid]
			if !ok {
				continue
			}
			if pc.DisplayName != "" {
				playerNames = append(playerNames, pc.DisplayName)
			} else {
				playerNames = append(playerNames, "玩家")
			}
		}
		rooms = append(rooms, protocol.StatusRoom{
			ID:          room.ID,
			Name:        "匹配房间",
			PlayerCount: len(room.PlayerIDs),
			MaxPlayers:  room.MaxPlayers,
			WaitSeconds: waitSec,
			PlayerNames: playerNames,
		})
	}
	msg := protocol.StatusMsg(online, matching, inGame, rooms)
	for _, c := range m.clients {
		safeSend(c.Send, msg)
	}
}
