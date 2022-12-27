package dbresolver

import (
	"context"
	"math/rand"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestRandomLoadBalancer_Apply(t *testing.T) {
	t.Run("only one db given", func(t *testing.T) {
		mockDB, _, err := sqlmock.New()
		assert.NoError(t, err)
		expectedDB := sqlx.NewDb(mockDB, "sqlmock")
		input := []*sqlx.DB{expectedDB}

		r := NewRandomLoadBalancer()
		result := r.Select(context.Background(), input)

		assert.Equal(t, expectedDB, result)
	})

	t.Run("multiple dbs given", func(t *testing.T) {
		mockDB, _, err := sqlmock.New()
		assert.NoError(t, err)
		expectedDB := sqlx.NewDb(mockDB, "sqlmock")
		input := []*sqlx.DB{
			expectedDB, sqlx.NewDb(mockDB, "foo"), sqlx.NewDb(mockDB, "bar"),
		}

		r := &RandomLoadBalancer{
			random: rand.New(rand.NewSource(0)),
		}
		result := r.Select(context.Background(), input)

		assert.Equal(t, expectedDB, result)
	})
}
