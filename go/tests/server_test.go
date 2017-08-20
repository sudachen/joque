package tests

import (
	"testing"

	. "github.com/sudachen/joque/go/server"
)

func TestServer1(t *testing.T) {
	srv, err := StartJoqueServer("localhost:9100")
	if err != nil {
		t.Errorf("failed to start JoqueServer: %s", err.Error())
		return
	}

	srv.Stop()
}
