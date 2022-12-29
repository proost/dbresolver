package dbresolver

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// errors.
var (
	errSelectedStmtNotFound = errors.New("dbresolver: selected stmt not found")
)

// Stmt is a wrapper around sqlx.Stmt.
type Stmt interface {
	Get(dest interface{}, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, args ...interface{}) error
	MustExec(args ...interface{}) sql.Result
	MustExecContext(ctx context.Context, args ...interface{}) sql.Result
	QueryRowx(args ...interface{}) *sqlx.Row
	QueryRowxContext(ctx context.Context, args ...interface{}) *sqlx.Row
	Queryx(args ...interface{}) (*sqlx.Rows, error)
	QueryxContext(ctx context.Context, args ...interface{}) (*sqlx.Rows, error)
	Select(dest interface{}, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, args ...interface{}) error
	Unsafe() *sqlx.Stmt
}

type stmt struct {
	primaries []*sqlx.DB
	reads     []*sqlx.DB

	primaryStmts map[*sqlx.DB]*sqlx.Stmt
	readStmts    map[*sqlx.DB]*sqlx.Stmt

	loadBalancer LoadBalancer
}

var _ Stmt = (*stmt)(nil)

// Get executes a prepared statement with the given arguments using readable db and returns.
// Get is a wrapper around sqlx.Stmt.Get.
func (s *stmt) Get(dest interface{}, args ...interface{}) error {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return errors.Wrapf(errSelectedStmtNotFound, "readable db: %v", db)
	}
	return stmt.Get(dest, args...)
}

// GetContext executes a prepared statement with the given arguments using readable db and returns.
// GetContext is a wrapper around sqlx.Stmt.GetContext.
func (s *stmt) GetContext(ctx context.Context, dest interface{}, args ...interface{}) error {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return errors.Wrapf(errSelectedStmtNotFound, "readable db: %v", db)
	}
	return stmt.GetContext(ctx, dest, args...)
}

// MustExec executes a prepared statement with the given arguments using primary db and returns.
// MustExec is a wrapper around sqlx.Stmt.MustExec.
func (s *stmt) MustExec(args ...interface{}) sql.Result {
	db := s.loadBalancer.Select(context.Background(), s.primaries)
	stmt, ok := s.primaryStmts[db]
	if !ok {
		// Should not happen.
		panic(errors.Wrapf(errSelectedStmtNotFound, "primary db: %v", db))
	}
	return stmt.MustExec(args...)
}

// MustExecContext executes a prepared statement with the given arguments using primary db and returns.
// MustExecContext is a wrapper around sqlx.Stmt.MustExecContext.
func (s *stmt) MustExecContext(ctx context.Context, args ...interface{}) sql.Result {
	db := s.loadBalancer.Select(ctx, s.primaries)
	stmt, ok := s.primaryStmts[db]
	if !ok {
		// Should not happen.
		panic(errors.Wrapf(errSelectedStmtNotFound, "primary db: %v", db))
	}
	return stmt.MustExecContext(ctx, args...)
}

// QueryRowx executes a prepared statement with the given arguments using readable db and returns.
// If selected statement is not found, returns nil.
// QueryRowx is a wrapper around sqlx.Stmt.QueryRowx.
func (s *stmt) QueryRowx(args ...interface{}) *sqlx.Row {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	return stmt.QueryRowx(args...)
}

// QueryRowxContext executes a prepared statement with the given arguments using readable db and returns.
// If selected statement is not found, returns nil.
// QueryRowxContext is a wrapper around sqlx.Stmt.QueryRowxContext.
func (s *stmt) QueryRowxContext(ctx context.Context, args ...interface{}) *sqlx.Row {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	return stmt.QueryRowxContext(ctx, args...)
}

// Queryx executes a prepared statement with the given arguments using readable db and returns.
// Queryx is a wrapper around sqlx.Stmt.Queryx.
func (s *stmt) Queryx(args ...interface{}) (*sqlx.Rows, error) {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Wrapf(errSelectedStmtNotFound, "readable db: %v", db)
	}
	return stmt.Queryx(args...)
}

// QueryxContext executes a prepared statement with the given arguments using readable db and returns.
// QueryxContext is a wrapper around sqlx.Stmt.QueryxContext.
func (s *stmt) QueryxContext(ctx context.Context, args ...interface{}) (*sqlx.Rows, error) {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Wrapf(errSelectedStmtNotFound, "readable db: %v", db)
	}
	return stmt.QueryxContext(ctx, args...)
}

// Select executes a prepared statement with the given arguments using readable db and returns.
// Select is a wrapper around sqlx.Stmt.Select.
func (s *stmt) Select(dest interface{}, args ...interface{}) error {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return errors.Wrapf(errSelectedStmtNotFound, "readable db: %v", db)
	}
	return stmt.Select(dest, args...)
}

// SelectContext executes a prepared statement with the given arguments using readable db and returns.
// SelectContext is a wrapper around sqlx.Stmt.SelectContext.
func (s *stmt) SelectContext(ctx context.Context, dest interface{}, args ...interface{}) error {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return errors.Wrapf(errSelectedStmtNotFound, "readable db: %v", db)
	}
	return stmt.SelectContext(ctx, dest, args...)
}

// Unsafe returns the underlying sqlx.Stmt.
// If selected statement is not found, returns nil.
// Unsafe wraps sqlx.Stmt.Unsafe.
func (s *stmt) Unsafe() *sqlx.Stmt {
	db := s.loadBalancer.Select(context.Background(), s.primaries)
	stmt, ok := s.primaryStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	return stmt.Unsafe()
}
