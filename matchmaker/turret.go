package matchmaker

// SetClientTurretStyle 设置当前连接玩家的炮塔样式（已钳制到合法范围），并触发状态广播。
// styleID 应为 1~7；若需与大厅/房间 UI 联动，可后续在 protocol.StatusRoom 中透出。
func (m *Matchmaker) SetClientTurretStyle(id string, styleID int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.clients[id]
	if !ok {
		return
	}
	c.TurretStyleID = styleID
	m.broadcastStatusLocked()
}
