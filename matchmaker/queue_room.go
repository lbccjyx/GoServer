package matchmaker

import (
	"log"

	"GoServer/protocol"
)

func (m *Matchmaker) LeaveRoom(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handleLeaveLocked(id)
	m.tryMatchLocked()
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

func (m *Matchmaker) findJoinableWaitingRoomLocked() *Room {
	var best *Room
	for _, room := range m.rooms {
		if room == nil || room.Started {
			continue
		}
		if len(room.PlayerIDs) >= room.MaxPlayers {
			continue
		}
		if best == nil || room.CreatedAt.Before(best.CreatedAt) {
			best = room
		}
	}
	return best
}

func (m *Matchmaker) attachWaitingClientToRoomLocked(c *Client, room *Room) {
	if c == nil || room == nil || room.Started {
		return
	}
	for _, pid := range room.PlayerIDs {
		if pid == c.ID {
			c.State = StateWaiting
			c.RoomID = room.ID
			return
		}
	}
	room.PlayerIDs = append(room.PlayerIDs, c.ID)
	c.State = StateWaiting
	c.RoomID = room.ID
	log.Printf("[matchmaker] client %s joined waiting room=%s (%d/%d)", c.ID, room.ID, len(room.PlayerIDs), room.MaxPlayers)
}

func (m *Matchmaker) dequeueOneWaitingLocked() (string, bool) {
	for i, id := range m.queue {
		if c, ok := m.clients[id]; ok && c.State == StateWaiting {
			delete(m.queueSet, id)
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			return id, true
		}
	}
	return "", false
}

func (m *Matchmaker) cancelFromWaitingRoomLocked(id string) bool {
	c, ok := m.clients[id]
	if !ok || c.State != StateWaiting || c.RoomID == "" {
		return false
	}
	room, ok := m.rooms[c.RoomID]
	if !ok || room.Started {
		return false
	}
	c.State = StateIdle
	c.RoomID = ""
	next := make([]string, 0, len(room.PlayerIDs))
	for _, pid := range room.PlayerIDs {
		if pid != id {
			next = append(next, pid)
		}
	}
	room.PlayerIDs = next
	if room.OwnerID == id {
		if len(room.PlayerIDs) > 0 {
			room.OwnerID = room.PlayerIDs[0]
		} else {
			room.OwnerID = ""
		}
	}
	if len(room.PlayerIDs) == 0 {
		m.disposeWaitingRoomLocked(room)
		log.Printf("[matchmaker] waiting room %s removed (empty)", room.ID)
		return true
	}
	if len(room.PlayerIDs) == 1 {
		lone := room.PlayerIDs[0]
		if pc, ok := m.clients[lone]; ok {
			safeSend(pc.Send, protocol.RoomDissolvedMsg(RoomDissolveTooFewPlayers))
			pc.State = StateIdle
			pc.RoomID = ""
		}
		m.disposeWaitingRoomLocked(room)
		log.Printf("[matchmaker] waiting room %s dissolved (only 1 player left)", room.ID)
		return true
	}
	m.notifyRoomWaitingLocked(room)
	return true
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
	indices := make([]int, 0, n)

	for i, id := range m.queue {
		if len(result) >= n {
			break
		}
		if c, ok := m.clients[id]; ok && c.State == StateWaiting {
			result = append(result, id)
			indices = append(indices, i)
		}
	}

	if len(result) < n {
		return []string{}
	}

	remove := make(map[int]bool, len(indices))
	for _, idx := range indices {
		remove[idx] = true
	}
	newQueue := make([]string, 0, len(m.queue)-len(indices))
	for i, id := range m.queue {
		if remove[i] {
			delete(m.queueSet, id)
			continue
		}
		newQueue = append(newQueue, id)
	}
	m.queue = newQueue
	return result
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
