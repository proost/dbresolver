package dbresolver

import (
	"net"

	"github.com/pkg/errors"
)

func isDBConnectionError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	var opErr *net.OpError
	return errors.As(err, &opErr)
}
