package server

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"

	"GoServer/matchmaker"
	"GoServer/protocol"
)

const (
	defaultServerVersion = "1.0.0"
	clientVersionFile    = "ClientVersion.txt"
	nameStoreFile        = "name_store.bin"
	downloadURL          = "http://121.41.191.154/"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var clientCounter atomic.Uint64

type Server struct {
	mm     *matchmaker.Matchmaker
	server *http.Server
	wg     sync.WaitGroup
	mu     sync.Mutex
	// 匹配服内存临时存储：按 IP 哈希保存最近一次昵称
	nameByIPHash  map[string]string
	nameStorePath string
}

func New(addr string, mm *matchmaker.Matchmaker) *Server {
	storePath := resolveNameStorePath()
	s := &Server{
		mm:            mm,
		nameByIPHash:  make(map[string]string),
		nameStorePath: storePath,
	}
	s.loadNameStore()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)

	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	return s
}

func (s *Server) Start() error {
	log.Printf("[server] listening on %s", s.server.Addr)
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	log.Println("[server] shutting down...")
	s.saveNameStore()
	return s.server.Shutdown(ctx)
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[server] upgrade error: %v", err)
		return
	}

	id := fmt.Sprintf("client-%d", clientCounter.Add(1))
	ipHash := hashIP(remoteIPOnly(r.RemoteAddr))
	sendCh := make(chan []byte, 64)
	client := s.mm.Register(id, sendCh)
	s.mu.Lock()
	existingName := s.nameByIPHash[ipHash]
	s.mu.Unlock()
	if existingName != "" {
		s.mm.SetClientDisplayName(id, existingName)
	}

	s.wg.Add(2)
	go s.writePump(conn, sendCh, id)
	go s.readPump(conn, sendCh, client, id, ipHash)
}

func (s *Server) readPump(conn *websocket.Conn, sendCh chan []byte, client *matchmaker.Client, id string, ipHash string) {
	defer func() {
		s.mm.Unregister(id)
		close(sendCh)
		conn.Close()
		s.wg.Done()
		log.Printf("[server] client %s disconnected", id)
	}()

	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	versionChecked := false
	serverVersion := loadServerVersion()

	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[server] read error from %s: %v", id, err)
			}
			return
		}
		if msgType != websocket.TextMessage {
			safeSend(sendCh, protocol.ErrorMsg("only text frames accepted"))
			continue
		}

		msg, err := protocol.ParseClientMessage(data)
		if err != nil {
			safeSend(sendCh, protocol.ErrorMsg("invalid JSON"))
			continue
		}

		if msg.Type == protocol.TypeVersionCheck {
			if msg.Version != serverVersion {
				safeSend(sendCh, protocol.ErrorVersionMismatch(
					fmt.Sprintf("client version mismatch: client=%q server=%q", msg.Version, serverVersion),
					serverVersion,
					downloadURL,
				))
				return
			}
			versionChecked = true
			safeSend(sendCh, protocol.VersionOKMsg(serverVersion))
			continue
		}

		if !versionChecked {
			safeSend(sendCh, protocol.ErrorMsg("version check required"))
			return
		}

		switch msg.Type {
		case protocol.TypeRequestMatch:
			s.mm.RequestMatch(id)
		case protocol.TypeCancelMatch:
			s.mm.CancelMatch(id)
		case protocol.TypeLeaveRoom:
			s.mm.LeaveRoom(id)
		case protocol.TypeDissolveRoom:
			s.mm.DissolveRoom(id)
		case protocol.TypePlayerName:
			name := sanitizePlayerName(msg.Name)
			if name == "" {
				// 空 name 视为查询当前 IP 对应昵称
				s.mu.Lock()
				existing := s.nameByIPHash[ipHash]
				s.mu.Unlock()
				s.mm.SetClientDisplayName(id, existing)
				safeSend(sendCh, protocol.PlayerNameSavedMsg(existing, ipHash))
				continue
			}
			s.mu.Lock()
			s.nameByIPHash[ipHash] = name
			s.mu.Unlock()
			s.mm.SetClientDisplayName(id, name)
			safeSend(sendCh, protocol.PlayerNameSavedMsg(name, ipHash))
		default:
			safeSend(sendCh, protocol.ErrorMsg(fmt.Sprintf("unknown type: %d", msg.Type)))
		}
	}
}

func loadServerVersion() string {
	path := clientVersionFile
	if exePath, err := os.Executable(); err == nil {
		path = filepath.Join(filepath.Dir(exePath), clientVersionFile)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("[server] read %s failed, fallback to %s, err=%v", clientVersionFile, defaultServerVersion, err)
		return defaultServerVersion
	}

	version := strings.TrimSpace(string(data))
	if version == "" {
		log.Printf("[server] %s is empty, fallback to %s", clientVersionFile, defaultServerVersion)
		return defaultServerVersion
	}

	return version
}

func (s *Server) writePump(conn *websocket.Conn, sendCh chan []byte, id string) {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		conn.Close()
		s.wg.Done()
	}()

	for {
		select {
		case msg, ok := <-sendCh:
			if !ok {
				conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Printf("[server] write error to %s: %v", id, err)
				return
			}
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func safeSend(ch chan []byte, data []byte) {
	defer func() { recover() }()
	select {
	case ch <- data:
	default:
	}
}

var multiSpaceRE = regexp.MustCompile(`\s+`)

func sanitizePlayerName(raw string) string {
	name := strings.TrimSpace(raw)
	if name == "" {
		return ""
	}
	name = multiSpaceRE.ReplaceAllString(name, " ")
	const maxLen = 12
	if utf8.RuneCountInString(name) > maxLen {
		runes := []rune(name)
		name = string(runes[:maxLen])
	}
	return name
}

func remoteIPOnly(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

func hashIP(ip string) string {
	sum := sha1.Sum([]byte(ip))
	return hex.EncodeToString(sum[:8])
}

func resolveNameStorePath() string {
	if exePath, err := os.Executable(); err == nil {
		return filepath.Join(filepath.Dir(exePath), nameStoreFile)
	}
	return nameStoreFile
}

func (s *Server) loadNameStore() {
	data, err := os.ReadFile(s.nameStorePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[server] load name store failed: %v", err)
		}
		return
	}
	var saved map[string]string
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&saved); err != nil {
		log.Printf("[server] decode name store failed: %v", err)
		return
	}
	if saved == nil {
		return
	}
	s.nameByIPHash = saved
	log.Printf("[server] loaded %d nickname records from %s", len(saved), s.nameStorePath)
}

func (s *Server) saveNameStore() {
	s.mu.Lock()
	snapshot := make(map[string]string, len(s.nameByIPHash))
	for k, v := range s.nameByIPHash {
		snapshot[k] = v
	}
	s.mu.Unlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(snapshot); err != nil {
		log.Printf("[server] encode name store failed: %v", err)
		return
	}
	if err := os.WriteFile(s.nameStorePath, buf.Bytes(), 0o644); err != nil {
		log.Printf("[server] save name store failed: %v", err)
		return
	}
	log.Printf("[server] saved %d nickname records to %s", len(snapshot), s.nameStorePath)
}
