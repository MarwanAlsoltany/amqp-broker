//go:build ignore

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

// examples lists every example in the order they should run.
// Basics first, then topology/message/handler, middlewares in the same
// order as the Available Middlewares table in the root README, exchange
// patterns, then advanced topics.
var examples = []string{
	// basics
	"publish",
	"consume",
	"publish-consume",
	// fundamentals
	"topology",
	"message",
	"handler",
	// middlewares
	"middleware-logging",
	"middleware-metrics",
	"middleware-debug",
	"middleware-recovery",
	"middleware-fallback",
	"middleware-retry",
	"middleware-circuit-breaker",
	"middleware-concurrency",
	"middleware-rate-limit",
	"middleware-deduplication",
	"middleware-validation",
	"middleware-transform",
	"middleware-deadline",
	"middleware-timeout",
	"middleware-batch",
	"middleware-stack",
	// patterns
	"pattern-rpc",
	"pattern-work-queue",
	"pattern-pub-sub",
	"pattern-direct",
	"pattern-topic",
	"pattern-headers",
	// advanced
	"advanced-config",
	"advanced-graceful-shutdown",
	"advanced-error-recovery",
	"advanced-idempotency",
	"advanced-low-level",
	"advanced-production",
}

const (
	timeout     = 30 * time.Second
	colorCyan   = "\033[36m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorRed    = "\033[31m"
	colorReset  = "\033[0m"
)

func main() {
	_, self, _, _ := runtime.Caller(0)
	root := filepath.Dir(self)

	// build a lookup set from the registered slice
	known := make(map[string]bool, len(examples))
	for _, name := range examples {
		known[name] = true
	}

	// find directories on disk with a main.go that are not in the slice
	var extras []string
	if entries, err := os.ReadDir(root); err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			if known[name] {
				continue
			}
			if _, err := os.Stat(filepath.Join(root, name, "main.go")); err == nil {
				extras = append(extras, name)
			}
		}
	}

	// build the run queue: validate registered entries, append unregistered extras last
	type item struct {
		name  string
		extra bool
	}
	var queue []item

	for _, name := range examples {
		if _, err := os.Stat(filepath.Join(root, name, "main.go")); err != nil {
			fmt.Printf("\n%sWARN: %q is in the examples slice but not found on disk, remove it%s\n",
				colorYellow, name, colorReset)
			continue
		}
		queue = append(queue, item{name, false})
	}
	for _, name := range extras {
		queue = append(queue, item{name, true})
	}

	total := len(queue)
	passed, timedout, failed := 0, 0, 0
	failures := []string{}

	fmt.Printf("\nRunning %d examples\n\n", total)

	for i, item := range queue {
		if item.extra {
			fmt.Printf("%sWARN: %q is not in the examples slice, add it to maintain ordering%s\n\n",
				colorYellow, item.name, colorReset)
		}

		dir := filepath.Join(root, item.name)
		fmt.Printf("%s=== [%02d/%02d] START   %s ===%s\n", colorCyan, i+1, total, item.name, colorReset)

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		cmd := exec.CommandContext(ctx, "go", "run", "main.go")
		cmd.Dir = dir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		// put go run + the compiled binary in their own process group so that
		// killing the group on timeout also kills the spawned child binary
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Cancel = func() error {
			if cmd.Process != nil {
				return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
			return nil
		}
		cmd.WaitDelay = 3 * time.Second

		err := cmd.Run()
		cancel()

		if err != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				fmt.Printf("%s=== [%02d/%02d] TIMEOUT %s ===%s\n\n", colorYellow, i+1, total, item.name, colorReset)
				timedout++
			} else {
				fmt.Printf("%s=== [%02d/%02d] FAIL    %s: %v ===%s\n\n", colorRed, i+1, total, item.name, err, colorReset)
				failed++
				failures = append(failures, item.name)
			}
		} else {
			fmt.Printf("%s=== [%02d/%02d] PASS    %s ===%s\n\n", colorGreen, i+1, total, item.name, colorReset)
			passed++
		}
	}

	fmt.Printf("Results: %s%d passed%s", colorGreen, passed, colorReset)
	if timedout > 0 {
		fmt.Printf(", %s%d timed out%s", colorYellow, timedout, colorReset)
	}
	if failed > 0 {
		fmt.Printf(", %s%d failed%s", colorRed, failed, colorReset)
		for _, name := range failures {
			fmt.Printf("\n  %s- %s%s", colorRed, name, colorReset)
		}
	}
	fmt.Println()

	if failed > 0 {
		os.Exit(1)
	}
}
