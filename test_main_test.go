package broker

import (
	"log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// configure test logging
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	log.SetOutput(os.Stdout)

	code := m.Run() // execute tests

	os.Exit(code)
}
