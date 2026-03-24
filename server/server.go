package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"GoServer/matchmaker"
	"GoServer/protocol"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var clientCounter atomic.Uint64

type Server struct {
	mm     *matchmaker.Matchmaker
	server *http.Server
	wg     sync.WaitGroup
}

func New(addr string, mm *matchmaker.Matchmaker) *Server {
	s := &Server{mm: mm}

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
	return s.server.Shutdown(ctx)
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[server] upgrade error: %v", err)
		return
	}

	id := fmt.Sprintf("client-%d", clientCounter.Add(1))
	sendCh := make(chan []byte, 64)
	client := s.mm.Register(id, sendCh)

	s.wg.Add(2)
	go s.writePump(conn, sendCh, id)
	go s.readPump(conn, sendCh, client, id)
}

func (s *Server) readPump(conn *websocket.Conn, sendCh chan []byte, client *matchmaker.Client, id string) {
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

		switch msg.Type {
		case protocol.TypeRequestMatch:
			s.mm.RequestMatch(id)
		case protocol.TypeCancelMatch:
			s.mm.CancelMatch(id)
		case protocol.TypeLeaveRoom:
			s.mm.LeaveRoom(id)
		case protocol.TypeDissolveRoom:
			s.mm.DissolveRoom(id)
		default:
			safeSend(sendCh, protocol.ErrorMsg(fmt.Sprintf("unknown type: %d", msg.Type)))
		}
	}
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
