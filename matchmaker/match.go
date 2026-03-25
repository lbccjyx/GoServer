package matchmaker

import (
	"fmt"
	"log"
	"time"

	"GoServer/protocol"
)

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

	if c.State == StateWaiting && c.RoomID != "" {
		if room, ok := m.rooms[c.RoomID]; ok && !room.Started {
			m.notifyRoomWaitingLocked(room)
		}
		return
	}

	if room := m.findJoinableWaitingRoomLocked(); room != nil {
		m.attachWaitingClientToRoomLocked(c, room)
		m.notifyRoomWaitingLocked(room)
		if len(room.PlayerIDs) >= room.MaxPlayers {
			m.startRoomLocked(room)
		}
		m.broadcastStatusLocked()
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

	if m.cancelFromWaitingRoomLocked(id) {
		if c, ok := m.clients[id]; ok {
			safeSend(c.Send, protocol.CancelMatchOKMsg())
		}
		log.Printf("[matchmaker] client %s cancelled waiting-room match", id)
		m.tryMatchLocked()
		m.broadcastStatusLocked()
		return
	}

	m.removeFromQueueLocked(id)
	if c, ok := m.clients[id]; ok && c.State == StateWaiting {
		c.State = StateIdle
		c.RoomID = ""
	}
	if c, ok := m.clients[id]; ok {
		safeSend(c.Send, protocol.CancelMatchOKMsg())
	}
	log.Printf("[matchmaker] client %s cancelled match", id)
	m.broadcastStatusLocked()
}

// tryMatchLocked 规则：
// 1) 先把排队玩家尽量塞进“已创建但未开局”的等待房（优先填满）。
// 2) 若没有等待房且队列 >=2，创建新等待房（先拉 2 人进房，开始 10 秒倒计时）。
// 3) 等待房满 4 人立即开局；否则超时后开局（至少 2 人）。
func (m *Matchmaker) tryMatchLocked() {
	for {
		room := m.findJoinableWaitingRoomLocked()
		if room == nil {
			break
		}
		id, ok := m.dequeueOneWaitingLocked()
		if !ok {
			break
		}
		c, exists := m.clients[id]
		if !exists || c.State != StateWaiting {
			continue
		}
		m.attachWaitingClientToRoomLocked(c, room)
		m.notifyRoomWaitingLocked(room)
		if len(room.PlayerIDs) >= room.MaxPlayers {
			m.startRoomLocked(room)
		}
	}

	for m.findJoinableWaitingRoomLocked() == nil {
		ids := m.dequeueNLocked(roomMinPlayers)
		if len(ids) < roomMinPlayers {
			break
		}
		room := m.createWaitingRoomLocked(ids)
		m.notifyRoomWaitingLocked(room)
	}
}

func (m *Matchmaker) createWaitingRoomLocked(ids []string) *Room {
	roomID := fmt.Sprintf("room-wait-%d", m.nextRoomID)
	m.nextRoomID++
	now := time.Now()
	room := &Room{
		ID:         roomID,
		OwnerID:    ids[0],
		PlayerIDs:  append([]string{}, ids...),
		MaxPlayers: roomMaxPlayers,
		CreatedAt:  now,
		WaitUntil:  now.Add(roomWaitBeforeStart),
		Started:    false,
	}
	m.rooms[roomID] = room
	for _, pid := range ids {
		if c, ok := m.clients[pid]; ok {
			c.State = StateWaiting
			c.RoomID = roomID
		}
	}
	go m.waitAndStartRoom(roomID, room.WaitUntil)
	log.Printf("[matchmaker] created waiting room=%s players=%v wait=%s", roomID, ids, roomWaitBeforeStart)
	return room
}

func (m *Matchmaker) waitAndStartRoom(roomID string, deadline time.Time) {
	earlyAt := deadline.Add(-roomDedicatedWarmupBeforeEnd)

	if d := time.Until(earlyAt); d > 0 {
		time.Sleep(d)
	}
	m.mu.Lock()
	if room, ok := m.rooms[roomID]; ok && !room.Started && room.WaitUntil.Equal(deadline) {
		m.startDedicatedEarlyLocked(room)
	}
	m.mu.Unlock()

	if d := time.Until(deadline); d > 0 {
		time.Sleep(d)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	room, ok := m.rooms[roomID]
	if !ok || room.Started {
		return
	}
	if !room.WaitUntil.Equal(deadline) {
		return
	}
	m.startRoomLocked(room)
	m.broadcastStatusLocked()
}

// startDedicatedEarlyLocked 须在 m.mu 下调用：倒计时第 8 秒起专服（先占端口）。
func (m *Matchmaker) startDedicatedEarlyLocked(room *Room) {
	if room == nil || room.Started {
		return
	}
	if len(room.PlayerIDs) < roomMinPlayers {
		return
	}
	if room.Port == 0 {
		room.Port = m.allocPortLocked()
	}
	go m.startDedicatedServer(room)
}

func (m *Matchmaker) startRoomLocked(room *Room) {
	if room == nil || room.Started {
		return
	}
	if len(room.PlayerIDs) < roomMinPlayers {
		return
	}
	if room.Port == 0 {
		room.Port = m.allocPortLocked()
	}
	room.Started = true
	room.WaitUntil = time.Now()
	matchMsg := protocol.MatchSuccessMsg(room.Port, len(room.PlayerIDs))
	for _, pid := range room.PlayerIDs {
		if c, ok := m.clients[pid]; ok {
			c.State = StateInRoom
			c.RoomID = room.ID
			safeSend(c.Send, matchMsg)
		}
	}
	go m.startDedicatedServer(room)
	log.Printf("[matchmaker] start room=%s port=%d players=%v", room.ID, room.Port, room.PlayerIDs)
}

func (m *Matchmaker) notifyRoomWaitingLocked(room *Room) {
	if room == nil || room.Started {
		return
	}
	waitSec := int(time.Until(room.WaitUntil).Seconds())
	if waitSec < 0 {
		waitSec = 0
	}
	ready := len(room.PlayerIDs) >= roomMinPlayers
	msg := protocol.RoomWaitingMsg(room.ID, len(room.PlayerIDs), room.MaxPlayers, waitSec, ready)
	for _, pid := range room.PlayerIDs {
		if c, ok := m.clients[pid]; ok {
			safeSend(c.Send, msg)
		}
	}
}
