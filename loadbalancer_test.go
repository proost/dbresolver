package dbresolver

import (
	"context"
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
}
