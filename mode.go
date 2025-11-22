package broker

import (
	"fmt"
	"log"
	"os"
)

type BrokerMode int

const (
	// ModeRelease disables verbose logging for production use.
	ModeRelease BrokerMode = iota
	// ModeDebug enables verbose logging for debugging purposes.
	ModeDebug
)

func (m BrokerMode) String() string {
	switch m {
	case ModeRelease:
		return "release"
	case ModeDebug:
		return "debug"
	default:
		return fmt.Sprintf("unknown(%d)", m)
	}
}

var Mode BrokerMode // defaults to ModeRelease

var logger *log.Logger

func init() {
	logger = log.New(os.Stdout, "[broker] ", log.Ltime|log.Lmicroseconds|log.Lshortfile|log.LUTC|log.Lmsgprefix)
}

// // log prints a log message if verbose mode is enabled.
// func logger.Printf(format string, v ...interface{}) {
// 	if Mode != ModeDebug {
// 		return
// 	}
// 	logger.Printf("%s\n", fmt.Sprintf(format, v...))
// }
