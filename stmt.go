package dbresolver

import (
	"context"
	"database/sql"

	"github.com/hashicorp/go-multierror"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// errors.
var (
	errSelectedStmtNotFound = errors.New("dbresolver: selected stmt not found")
)

// Stmt is a wrapper around sqlx.Stmt.
type Stmt interface {
	Close() error
	Exec(args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error)
	Get(dest interface{}, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, args ...interface{}) error
	MustExec(args ...interface{}) sql.Result
	MustExecContext(ctx context.Context, args ...interface{}) sql.Result
	Query(args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error)
	QueryRow(args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row
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

// Close closes all statements.
// Close is a wrapper around sqlx.Stmt.Close.
func (s *stmt) Close() error {
	var errs error
	for _, stmt := range s.primaryStmts {
		if err := stmt.Close(); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	for _, stmt := range s.readStmts {
		if err := stmt.Close(); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	return errs
}

// Exec chooses a primary database's statement and executes using chosen statement.
// Exec is a wrapper around sqlx.Stmt.Exec.
func (s *stmt) Exec(args ...interface{}) (sql.Result, error) {
	db := s.loadBalancer.Select(context.Background(), s.primaries)
	stmt, ok := s.primaryStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Wrapf(errSelectedStmtNotFound, "primary db: %v", db)
	}
	return stmt.Exec(args...)
}

// ExecContext chooses a primary database's statement and executes using chosen statement.
// ExecContext is a wrapper around sqlx.Stmt.ExecContext.
func (s *stmt) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	db := s.loadBalancer.Select(ctx, s.primaries)
	stmt, ok := s.primaryStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Wrapf(errSelectedStmtNotFound, "primary db: %v", db)
	}
	return stmt.ExecContext(ctx, args...)
}

// Get chooses a readable database's statement and Get using chosen statement.
// Get is a wrapper around sqlx.Stmt.Get.
func (s *stmt) Get(dest interface{}, args ...interface{}) error {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return errors.Wrapf(errSelectedStmtNotFound, "readable db: %v", db)
	}
	err := stmt.Get(dest, args...)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(context.Background(), s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		err = stmtPrimary.Get(dest, args...)
	}
	return err
}

// GetContext chooses a readable database's statement and Get using chosen statement.
// GetContext is a wrapper around sqlx.Stmt.GetContext.
func (s *stmt) GetContext(ctx context.Context, dest interface{}, args ...interface{}) error {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return errors.Wrapf(errSelectedStmtNotFound, "readable db: %v", db)
	}
	err := stmt.GetContext(ctx, dest, args...)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(ctx, s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		err = stmtPrimary.GetContext(ctx, dest, args...)
	}
	return err
}

// MustExec chooses a primary database's statement and executes using chosen statement or panic.
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

// MustExecContext chooses a primary database's statement and executes using chosen statement or panic.
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

// Query chooses a readable database's statement and executes using chosen statement.
// Query is a wrapper around sqlx.Stmt.Query.
func (s *stmt) Query(args ...interface{}) (*sql.Rows, error) {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Wrapf(errSelectedStmtNotFound, "readable db: %v", db)
	}
	rows, err := stmt.Query(args...)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(context.Background(), s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		rows, err = stmtPrimary.Query(args...)
	}
	return rows, err
}

// QueryContext chooses a readable database's statement and executes using chosen statement.
// QueryContext is a wrapper around sqlx.Stmt.QueryContext.
func (s *stmt) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Wrapf(errSelectedStmtNotFound, "readable db: %v", db)
	}
	rows, err := stmt.QueryContext(ctx, args...)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(ctx, s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		rows, err = stmtPrimary.QueryContext(ctx, args...)
	}
	return rows, err
}

// QueryRow chooses a readable database's statement, executes using chosen statement and returns *sqlx.Row.
// If selected statement is not found, returns nil.
// QueryRow is a wrapper around sqlx.Stmt.QueryRow.
func (s *stmt) QueryRow(args ...interface{}) *sql.Row {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	row := stmt.QueryRow(args...)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.loadBalancer.Select(context.Background(), s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRow(args...)
	}
	return row
}

// QueryRowContext chooses a readable database's statement, executes using chosen statement and returns *sqlx.Row.
// If selected statement is not found, returns nil.
// QueryRowContext is a wrapper around sqlx.Stmt.QueryRowContext.
func (s *stmt) QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	row := stmt.QueryRowContext(ctx, args...)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.loadBalancer.Select(ctx, s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRowContext(ctx, args...)
	}
	return row
}

// QueryRowx chooses a readable database's statement, executes using chosen statement and returns *sqlx.Row.
// If selected statement is not found, returns nil.
// QueryRowx is a wrapper around sqlx.Stmt.QueryRowx.
func (s *stmt) QueryRowx(args ...interface{}) *sqlx.Row {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	row := stmt.QueryRowx(args...)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.loadBalancer.Select(context.Background(), s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRowx(args...)
	}
	return row
}

// QueryRowxContext chooses a readable database's statement, executes using chosen statement and returns *sqlx.Row.
// If selected statement is not found, returns nil.
// QueryRowxContext is a wrapper around sqlx.Stmt.QueryRowxContext.
func (s *stmt) QueryRowxContext(ctx context.Context, args ...interface{}) *sqlx.Row {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil
	}
	row := stmt.QueryRowxContext(ctx, args...)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.loadBalancer.Select(ctx, s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRowxContext(ctx, args...)
	}
	return row
}

// Queryx chooses a readable database's statement, executes using chosen statement and returns *sqlx.Rows.
// Queryx is a wrapper around sqlx.Stmt.Queryx.
func (s *stmt) Queryx(args ...interface{}) (*sqlx.Rows, error) {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Wrapf(errSelectedStmtNotFound, "readable db: %v", db)
	}
	rows, err := stmt.Queryx(args...)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(context.Background(), s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		rows, err = stmtPrimary.Queryx(args...)
	}
	return rows, err
}

// QueryxContext chooses a readable database's statement, executes using chosen statement and returns *sqlx.Rows.
// QueryxContext is a wrapper around sqlx.Stmt.QueryxContext.
func (s *stmt) QueryxContext(ctx context.Context, args ...interface{}) (*sqlx.Rows, error) {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return nil, errors.Wrapf(errSelectedStmtNotFound, "readable db: %v", db)
	}
	rows, err := stmt.QueryxContext(ctx, args...)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(ctx, s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		rows, err = stmtPrimary.QueryxContext(ctx, args...)
	}
	return rows, err
}

// Select chooses a readable database's statement, executes using chosen statement.
// Select is a wrapper around sqlx.Stmt.Select.
func (s *stmt) Select(dest interface{}, args ...interface{}) error {
	db := s.loadBalancer.Select(context.Background(), s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return errors.Wrapf(errSelectedStmtNotFound, "readable db: %v", db)
	}
	err := stmt.Select(dest, args...)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(context.Background(), s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		err = stmtPrimary.Select(dest, args...)
	}
	return err
}

// SelectContext chooses a readable database's statement, executes using chosen statement.
// SelectContext is a wrapper around sqlx.Stmt.SelectContext.
func (s *stmt) SelectContext(ctx context.Context, dest interface{}, args ...interface{}) error {
	db := s.loadBalancer.Select(ctx, s.reads)
	stmt, ok := s.readStmts[db]
	if !ok {
		// Should not happen.
		return errors.Wrapf(errSelectedStmtNotFound, "readable db: %v", db)
	}
	err := stmt.SelectContext(ctx, dest, args...)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(ctx, s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		err = stmtPrimary.SelectContext(ctx, dest, args...)
	}
	return err
}

// Unsafe chooses a primary database's statement and returns underlying sql.Stmt.
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
