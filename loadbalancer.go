package dbresolver

import (
	"context"
	"math/rand"

	"github.com/jmoiron/sqlx"
)

// LoadBalancer chooses a database from the given databases.
type LoadBalancer interface {
	// Select returns the database to use for the given operation.
	Select(ctx context.Context, dbs []*sqlx.DB) *sqlx.DB
}

// RandomLoadBalancer is a load balancer that chooses a database randomly.
type RandomLoadBalancer struct{}

var _ LoadBalancer = (*RandomLoadBalancer)(nil)

func NewRandomLoadBalancer() *RandomLoadBalancer {
	return &RandomLoadBalancer{}
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
	return dbs[rand.Intn(n)]
}

// injectedLoadBalancer is a load balancer that always chooses the given database.
// It is used for testing.
type injectedLoadBalancer struct {
	db *sqlx.DB
}

var _ LoadBalancer = (*injectedLoadBalancer)(nil)

func (b *injectedLoadBalancer) Select(_ context.Context, _ []*sqlx.DB) *sqlx.DB {
	return b.db
}
