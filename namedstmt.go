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
	var errs error
	for _, pStmt := range s.primaryStmts {
		err := pStmt.Close()
		if err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	for _, rStmt := range s.readStmts {
		err := rStmt.Close()
		if err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	if errs != nil {
		return errs
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
	err := stmt.Get(dest, arg)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(context.Background(), s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		err = stmtPrimary.Get(dest, arg)
	}
	return err
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
	err := stmt.GetContext(ctx, dest, arg)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(ctx, s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		err = stmtPrimary.GetContext(ctx, dest, arg)
	}
	return err
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
	rows, err := stmt.Query(arg)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(context.Background(), s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		rows, err = stmtPrimary.Query(arg)
	}
	return rows, err
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
	rows, err := stmt.QueryContext(ctx, arg)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(ctx, s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		rows, err = stmtPrimary.QueryContext(ctx, arg)
	}
	return rows, err
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
	row := stmt.QueryRow(arg)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.loadBalancer.Select(context.Background(), s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRow(arg)
	}
	return row
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
	row := stmt.QueryRowContext(ctx, arg)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.loadBalancer.Select(ctx, s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRowContext(ctx, arg)
	}
	return row
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
	row := stmt.QueryRowx(arg)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.loadBalancer.Select(context.Background(), s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRowx(arg)
	}
	return row
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
	row := stmt.QueryRowxContext(ctx, arg)

	if isDBConnectionError(row.Err()) {
		dbPrimary := s.loadBalancer.Select(ctx, s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil
		}
		row = stmtPrimary.QueryRowxContext(ctx, arg)
	}
	return row
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
	rows, err := stmt.Queryx(arg)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(context.Background(), s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		rows, err = stmtPrimary.Queryx(arg)
	}
	return rows, err
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
	rows, err := stmt.QueryxContext(ctx, arg)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(ctx, s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return nil, errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		rows, err = stmtPrimary.QueryxContext(ctx, arg)
	}
	return rows, err
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
	err := stmt.Select(dest, arg)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(context.Background(), s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		err = stmtPrimary.Select(dest, arg)
	}
	return err
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
	err := stmt.SelectContext(ctx, dest, arg)

	if isDBConnectionError(err) {
		dbPrimary := s.loadBalancer.Select(ctx, s.primaries)
		stmtPrimary, ok := s.readStmts[dbPrimary]
		if !ok {
			// Should not happen.
			return errors.Wrapf(errSelectedNamedStmtNotFound, "readable db: %v", db)
		}
		err = stmtPrimary.SelectContext(ctx, dest, arg)
	}
	return err
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
