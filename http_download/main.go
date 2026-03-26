package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	listenAddr       = ":80"
	downloadPath     = "扑鱼达人.exe"
	downloadSecret   = "123456"
	downloadCooldown = 3 * time.Minute
)

var (
	mu           sync.Mutex
	lastDownload = make(map[string]time.Time)
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)

	if _, err := os.Stat(downloadPath); err != nil {
		log.Fatalf("下载文件不存在: %s, err: %v", downloadPath, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", indexHandler)
	mux.HandleFunc("/download", downloadHandler)

	log.Printf("下载服务启动: http://0.0.0.0%s", listenAddr)
	log.Printf("目标文件: %s", downloadPath)
	if err := http.ListenAndServe(listenAddr, mux); err != nil {
		log.Fatalf("服务启动失败: %v", err)
	}
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(`<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>文件下载</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 40px; }
    .card { max-width: 420px; padding: 20px; border: 1px solid #ddd; border-radius: 8px; }
    input, button { width: 100%; padding: 10px; margin-top: 10px; }
    button { cursor: pointer; }
    .hint { color: #666; font-size: 12px; margin-top: 8px; }
  </style>
</head>
<body>
  <div class="card">
    <h3>扑鱼达人下载</h3>
    <form method="post" action="/download">
      <input type="password" name="password" placeholder="请输入下载密码" required />
      <button type="submit">下载扑鱼达人.exe</button>
    </form>
    <div class="hint">同一 IP 每 3 分钟仅允许下载 1 次。</div>
  </div>
</body>
</html>`))
}

func downloadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "仅支持 POST", http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Error(w, "请求参数错误", http.StatusBadRequest)
		return
	}

	password := strings.TrimSpace(r.FormValue("password"))
	if password != downloadSecret {
		http.Error(w, "下载密码错误", http.StatusForbidden)
		return
	}

	ip := clientIP(r)
	if ip == "" {
		http.Error(w, "无法识别客户端 IP", http.StatusBadRequest)
		return
	}

	if wait, blocked := checkCooldown(ip); blocked {
		http.Error(w, fmt.Sprintf("下载过于频繁，请在 %d 秒后重试", int(wait.Seconds())+1), http.StatusTooManyRequests)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filepath.Base(downloadPath)))
	http.ServeFile(w, r, downloadPath)
}

func checkCooldown(ip string) (time.Duration, bool) {
	now := time.Now()

	mu.Lock()
	defer mu.Unlock()

	if last, ok := lastDownload[ip]; ok {
		if now.Sub(last) < downloadCooldown {
			return downloadCooldown - now.Sub(last), true
		}
	}

	lastDownload[ip] = now
	return 0, false
}

func clientIP(r *http.Request) string {
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		parts := strings.Split(xff, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
	}

	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil {
		return host
	}
	return strings.TrimSpace(r.RemoteAddr)
}
