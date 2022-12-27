package dbresolver

import (
	"context"
	"math/rand"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	random = rand.New(rand.NewSource(time.Now().Unix()))
)

// LoadBalancer chooses a database from the given databases.
type LoadBalancer interface {
	// Select returns the database to use for the given operation.
	Select(ctx context.Context, dbs []*sqlx.DB) *sqlx.DB
}

// RandomLoadBalancer is a load balancer that chooses a database randomly.
type RandomLoadBalancer struct {
	random *rand.Rand
}

var _ LoadBalancer = (*RandomLoadBalancer)(nil)

func NewRandomLoadBalancer() *RandomLoadBalancer {
	return &RandomLoadBalancer{
		random: random,
	}
}

// Select returns the database to use for the given operation.
// If there are no databases, it returns nil. but it should not happen.
func (b *RandomLoadBalancer) Select(_ context.Context, dbs []*sqlx.DB) *sqlx.DB {
	n := len(dbs)
	if n == 0 {
		return nil
	}
	if n == 1 {
		return dbs[0]
	}
	return dbs[b.random.Intn(n)]
}
