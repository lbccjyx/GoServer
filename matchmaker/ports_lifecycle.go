package matchmaker

import (
	"log"
	"os/exec"
)

// roomLiveMatchmakerPeersLocked 统计「本房 PlayerIDs 里仍在 m.clients 且 StateInRoom 指向本房」的人数。
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
	if !ok {
		return
	}
	if c.State == StateWaiting && c.RoomID != "" {
		m.cancelFromWaitingRoomLocked(id)
		return
	}
	if c.State != StateInRoom {
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

// disposeWaitingRoomLocked 等待房被清空时回收专服与端口（可能已在第 8 秒预热启动）。
func (m *Matchmaker) disposeWaitingRoomLocked(room *Room) {
	if room == nil {
		return
	}
	if room.Process != nil && room.Process.Process != nil {
		_ = room.Process.Process.Kill()
		log.Printf("[matchmaker] killed warmup dedicated server on port %d (waiting room empty)", room.Port)
	}
	room.Process = nil
	if room.Port != 0 {
		m.freePorts = append(m.freePorts, room.Port)
	}
	delete(m.rooms, room.ID)
}

func (m *Matchmaker) recycleRoomLocked(room *Room) {
	if room.Process != nil && room.Process.Process != nil {
		_ = room.Process.Process.Kill()
		log.Printf("[matchmaker] killed dedicated server on port %d", room.Port)
	}
	room.Process = nil
	m.freePorts = append(m.freePorts, room.Port)
	delete(m.rooms, room.ID)
	log.Printf("[matchmaker] room %s recycled, port %d returned to pool (pool size=%d)", room.ID, room.Port, len(m.freePorts))
}

// dedicatedProcessDoneLocked 专服进程退出；持有 m.mu。
func (m *Matchmaker) dedicatedProcessDoneLocked(room *Room, cmd *exec.Cmd) {
	if r, ok := m.rooms[room.ID]; ok && r.Process == cmd {
		r.Process = nil
		if !r.Started {
			log.Printf("[matchmaker] dedicated warmup exited early for room %s (port %d), will retry at match start if needed", r.ID, r.Port)
			return
		}
		m.freePorts = append(m.freePorts, r.Port)
		delete(m.rooms, r.ID)
		log.Printf("[matchmaker] room %s released after dedicated exit, port %d returned to pool (pool size=%d)", r.ID, r.Port, len(m.freePorts))
	}
}
