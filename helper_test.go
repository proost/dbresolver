package dbresolver

import (
	"net"
	"testing"

	"github.com/pkg/errors"
)

func TestIsDBConnectionError(t *testing.T) {
	// test connection timeout error
	timeoutError := &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{IsTimeout: true}}
	if !isDBConnectionError(timeoutError) {
		t.Error("Expected true for timeout error")
	}

	// test general network error
	networkError := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("network error")}
	if !isDBConnectionError(networkError) {
		t.Error("Expected true for network error")
	}

	// test non-network error
	otherError := errors.New("other error")
	if isDBConnectionError(otherError) {
		t.Error("Expected false for non-network error")
	}
}
