package dbresolver

import "net"

func isDBConnectionError(err error) bool {
	if _, ok := err.(net.Error); ok {
		return ok
	}

	if _, ok := err.(*net.OpError); ok {
		return ok
	}
	return false
}
