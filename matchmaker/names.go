package matchmaker

// SetClientDisplayName 设置客户端昵称，并触发状态广播以刷新大厅房间列表中的人名。
func (m *Matchmaker) SetClientDisplayName(id string, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.clients[id]
	if !ok {
		return
	}
	c.DisplayName = name
	m.broadcastStatusLocked()
}
