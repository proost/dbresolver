package dbresolver

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// errors.
var (
	errSelectedNamedStmtNotFound = errors.New("dbresolver: selected named stmt not found")
)

// NamedStmt is a wrapper around sqlx.NamedStmt.
type NamedStmt interface {
	Close() error
	Exec(arg interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, arg interface{}) (sql.Result, error)
	Get(dest interface{}, arg interface{}) error
	GetContext(ctx context.Context, dest interface{}, arg interface{}) error
	MustExec(arg interface{}) sql.Result
	MustExecContext(ctx context.Context, arg interface{}) sql.Result
	Query(arg interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, arg interface{}) (*sql.Rows, error)
	QueryRow(arg interface{}) *sqlx.Row
	QueryRowContext(ctx context.Context, arg interface{}) *sqlx.Row
	QueryRowx(arg interface{}) *sqlx.Row
	QueryRowxContext(ctx context.Context, arg interface{}) *sqlx.Row
	Queryx(arg interface{}) (*sqlx.Rows, error)
	QueryxContext(ctx context.Context, arg interface{}) (*sqlx.Rows, error)
	Select(dest interface{}, arg interface{}) error
	SelectContext(ctx context.Context, dest interface{}, arg interface{}) error
	Unsafe() *sqlx.NamedStmt
}

type namedStmt struct {
	primaries []*sqlx.DB
	reads     []*sqlx.DB

	primaryStmts map[*sqlx.DB]*sqlx.NamedStmt
	readStmts    map[*sqlx.DB]*sqlx.NamedStmt

	loadBalancer LoadBalancer
}

// Close closes all primary database's named statements and readable database's named statements.
// Close wraps sqlx.NamedStmt.Close.
func (s *namedStmt) Close() error {
	g, _ := errgroup.WithContext(context.Background())

	for _, stmt := range s.primaryStmts {
		stmt := stmt
		g.Go(func() error {
			err := stmt.Close()
			if err != nil {
				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	for _, stmt := range s.readStmts {
		stmt := stmt
		g.Go(func() error {
			return stmt.Close()
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

// Exec chooses a primary database's named statement and executes a named statement given argument.
// Exec wraps sqlx.NamedStmt.Exec.
func (s *namedStmt) Exec(arg interface{}) (sql.Result, error) {
	db := s.loadBalancer.Select(context.Background(), s.primaries)
	stmt, ok := s.primaryStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Wrapf(errSelectedNamedStmtNotFound, "primary db: %v", db)
	}
	return stmt.Exec(arg)
}

// ExecContext chooses a primary database's named statement and executes a named statement given argument.
// ExecContext wraps sqlx.NamedStmt.ExecContext.
func (s *namedStmt) ExecContext(ctx context.Context, arg interface{}) (sql.Result, error) {
	db := s.loadBalancer.Select(ctx, s.primaries)
	stmt, ok := s.primaryStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Wrapf(errSelectedNamedStmtNotFound, "primary db: %v", db)
	}
	return stmt.ExecContext(ctx, arg)
}

// Get chooses a readable database's named statement and Get using chosen statement.
// Get wraps sqlx.NamedStmt.Get.
func (s *namedStmt) Get(dest interface{}, arg interface{}) error {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
	}
	return stmt.Get(dest, arg)
}

// GetContext chooses a readable database's named statement and Get using chosen statement.
// GetContext wraps sqlx.NamedStmt.GetContext.
func (s *namedStmt) GetContext(ctx context.Context, dest interface{}, arg interface{}) error {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
	}
	return stmt.GetContext(ctx, dest, arg)
}

// MustExec chooses a primary database's named statement
// and executes chosen statement with given argument.
// MustExec wraps sqlx.NamedStmt.MustExec.
func (s *namedStmt) MustExec(arg interface{}) sql.Result {
	db := s.loadBalancer.Select(context.Background(), s.primaries)
	stmt, ok := s.primaryStmts[db]
	if !ok {
		// Should not happen.
		panic(errors.Wrapf(errSelectedNamedStmtNotFound, "primary db: %v", db))
	}
	return stmt.MustExec(arg)
}

// MustExecContext chooses a primary database's named statement
// and executes chosen statement with given argument.
// MustExecContext wraps sqlx.NamedStmt.MustExecContext.
func (s *namedStmt) MustExecContext(ctx context.Context, arg interface{}) sql.Result {
	db := s.loadBalancer.Select(ctx, s.primaries)
	stmt, ok := s.primaryStmts[db]
	if !ok {
		// Should not happen.
		panic(errors.Wrapf(errSelectedNamedStmtNotFound, "primary db: %v", db))
	}
	return stmt.MustExecContext(ctx, arg)
}

// Query chooses a readable database's named statement, executes chosen statement with given argument
// and returns sql.Rows.
// Query wraps sqlx.NamedStmt.Query.
func (s *namedStmt) Query(arg interface{}) (*sql.Rows, error) {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
	}
	return stmt.Query(arg)
}

// QueryContext chooses a readable database's named statement, executes chosen statement with given argument
// and returns sql.Rows.
// QueryContext wraps sqlx.NamedStmt.QueryContext.
func (s *namedStmt) QueryContext(ctx context.Context, arg interface{}) (*sql.Rows, error) {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
	}
	return stmt.QueryContext(ctx, arg)
}

// QueryRow chooses a readable database's named statement, executes chosen statement with given argument
// and returns a *sqlx.Row
// If selected statement is not found, returns nil.
// QueryRow wraps sqlx.NamedStmt.QueryRow.
func (s *namedStmt) QueryRow(arg interface{}) *sqlx.Row {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	return stmt.QueryRow(arg)
}

// QueryRowContext chooses a readable database's named statement, executes chosen statement with given argument
// and returns a *sqlx.Row
// If selected statement is not found, returns nil.
// QueryRowContext wraps sqlx.NamedStmt.QueryRowContext.
func (s *namedStmt) QueryRowContext(ctx context.Context, arg interface{}) *sqlx.Row {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	return stmt.QueryRowContext(ctx, arg)
}

// QueryRowx chooses a readable database's named statement, executes chosen statement with given argument
// and returns a *sqlx.Row
// If selected statement is not found, returns nil.
// QueryRowx wraps sqlx.NamedStmt.QueryRowx.
func (s *namedStmt) QueryRowx(arg interface{}) *sqlx.Row {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	return stmt.QueryRowx(arg)
}

// QueryRowxContext chooses a readable database's named statement, executes chosen statement with given argument
// and returns a *sqlx.Row
// If selected statement is not found, returns nil.
// QueryRowxContext wraps sqlx.NamedStmt.QueryRowxContext.
func (s *namedStmt) QueryRowxContext(ctx context.Context, arg interface{}) *sqlx.Row {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	return stmt.QueryRowxContext(ctx, arg)
}

// Queryx chooses a readable database's named statement, executes chosen statement with given argument
// and returns sqlx.Rows.
// Queryx wraps sqlx.NamedStmt.Queryx.
func (s *namedStmt) Queryx(arg interface{}) (*sqlx.Rows, error) {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
	}
	return stmt.Queryx(arg)
}

// QueryxContext chooses a readable database's named statement, executes chosen statement with given argument
// and returns sqlx.Rows.
// QueryxContext wraps sqlx.NamedStmt.QueryxContext.
func (s *namedStmt) QueryxContext(ctx context.Context, arg interface{}) (*sqlx.Rows, error) {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
	}
	return stmt.QueryxContext(ctx, arg)
}

// Select chooses a readable database's named statement, executes chosen statement with given argument
// Select wraps sqlx.NamedStmt.Select.
func (s *namedStmt) Select(dest interface{}, arg interface{}) error {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
	}
	return stmt.Select(dest, arg)
}

// SelectContext chooses a readable database's named statement, executes chosen statement with given argument
// SelectContext wraps sqlx.NamedStmt.SelectContext.
func (s *namedStmt) SelectContext(ctx context.Context, dest interface{}, arg interface{}) error {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
	}
	return stmt.SelectContext(ctx, dest, arg)
}

// Unsafe chooses a primary database's named statement and returns the underlying sqlx.NamedStmt.
// If selected statement is not found, returns nil.
// Unsafe wraps sqlx.NamedStmt.Unsafe.
func (s *namedStmt) Unsafe() *sqlx.NamedStmt {
	db := s.loadBalancer.Select(context.Background(), s.primaries)
	stmt, ok := s.primaryStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	return stmt.Unsafe()
}
