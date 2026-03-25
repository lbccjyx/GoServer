package matchmaker

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// resolveDedicatedServerExe 返回专服可执行文件的绝对路径。
func resolveDedicatedServerExe() (string, error) {
	const defaultName = "DedicatedServer.exe"

	if p := os.Getenv("DEDICATED_SERVER"); p != "" {
		return filepath.Abs(filepath.Clean(p))
	}

	if self, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(self), defaultName)
		if _, err := os.Stat(candidate); err == nil {
			return filepath.Abs(candidate)
		}
	}

	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Abs(filepath.Join(wd, defaultName))
}

// resolveDedicatedServerLogDir 专服 stdout/stderr 日志目录。
func resolveDedicatedServerLogDir() (string, error) {
	if d := os.Getenv("DEDICATED_SERVER_LOG_DIR"); d != "" {
		return filepath.Abs(filepath.Clean(d))
	}
	if self, err := os.Executable(); err == nil {
		return filepath.Abs(filepath.Join(filepath.Dir(self), "logs"))
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Abs(filepath.Join(wd, "logs"))
}

func (m *Matchmaker) startDedicatedServer(room *Room) {
	m.mu.Lock()
	if room.Process != nil || room.dedicatedStartInProgress {
		m.mu.Unlock()
		return
	}
	room.dedicatedStartInProgress = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		room.dedicatedStartInProgress = false
		m.mu.Unlock()
	}()

	exePath, err := resolveDedicatedServerExe()
	if err != nil {
		log.Printf("[matchmaker] resolve dedicated server path: %v", err)
		return
	}
	if _, err := os.Stat(exePath); err != nil {
		log.Printf("[matchmaker] dedicated server not found: %q (%v). 设置环境变量 DEDICATED_SERVER 为完整路径，或与匹配服 exe 同目录放置 DedicatedServer.exe", exePath, err)
		return
	}

	logDir, err := resolveDedicatedServerLogDir()
	if err != nil {
		log.Printf("[matchmaker] dedicated server log dir: %v", err)
		logDir = "."
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		log.Printf("[matchmaker] mkdir log dir %q: %v", logDir, err)
	}

	started := time.Now()
	logName := fmt.Sprintf("DedicatedServer_%s_%09d_port%d.log", started.Format("20060102_150405"), started.Nanosecond(), room.Port)
	logPath := filepath.Join(logDir, logName)

	var logFile *os.File
	logFile, err = os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		log.Printf("[matchmaker] open dedicated log %q: %v (专服仍将启动，但日志不落盘)", logPath, err)
		logFile = nil
	} else {
		_, _ = fmt.Fprintf(logFile, "--- DedicatedServer start %s ---\nexe=%s\nargs=--port %d\nroom=%s\n\n",
			started.Format(time.RFC3339Nano), exePath, room.Port, room.ID)
	}

	cmd := exec.Command(exePath, "--port", fmt.Sprintf("%d", room.Port))
	if logFile != nil {
		cmd.Stdout = logFile
		cmd.Stderr = logFile
	}
	if err := cmd.Start(); err != nil {
		if logFile != nil {
			_, _ = fmt.Fprintf(logFile, "\n--- start failed: %v ---\n", err)
			_ = logFile.Close()
		}
		log.Printf("[matchmaker] failed to start dedicated server %q on port %d: %v", exePath, room.Port, err)
		return
	}

	m.mu.Lock()
	room.Process = cmd
	room.dedicatedStartInProgress = false
	m.mu.Unlock()

	log.Printf("[matchmaker] launching dedicated server: %s --port %d (log=%s)", exePath, room.Port, logPath)
	log.Printf("[matchmaker] dedicated server started on port %d, pid=%d", room.Port, cmd.Process.Pid)

	waitErr := cmd.Wait()
	exited := time.Now()
	if logFile != nil {
		_, _ = fmt.Fprintf(logFile, "\n--- DedicatedServer exited at %s (duration=%s) ---\n", exited.Format(time.RFC3339Nano), exited.Sub(started))
		if waitErr != nil {
			_, _ = fmt.Fprintf(logFile, "wait error: %v\n", waitErr)
		}
		if err := logFile.Close(); err != nil {
			log.Printf("[matchmaker] close dedicated log %q: %v", logPath, err)
		}
	}
	if waitErr != nil {
		log.Printf("[matchmaker] dedicated server on port %d exited: %v", room.Port, waitErr)
	}

	m.mu.Lock()
	m.dedicatedProcessDoneLocked(room, cmd)
	m.mu.Unlock()
}
