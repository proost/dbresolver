package dbresolver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// errors.
var (
	errNoPrimaryDB            = errors.New("dbresolver: no primary database")
	errInvalidReadWritePolicy = errors.New("dbresolver: invalid read/write policy")
	errNoDBToRead             = errors.New("dbresolver: no database to read")
)

// ReadWritePolicy is the read/write policy for the primary databases.
type ReadWritePolicy string

// ReadWritePolicies.
const (
	ReadWrite ReadWritePolicy = "read-write"
	WriteOnly ReadWritePolicy = "write-only"
)

var validReadWritePolicies = map[ReadWritePolicy]struct{}{
	ReadWrite: {},
	WriteOnly: {},
}

// PrimaryDBsConfig is the config of primary databases.
type PrimaryDBsConfig struct {
	DBs             []*sqlx.DB
	ReadWritePolicy ReadWritePolicy
}

// NewPrimaryDBsConfig creates a new PrimaryDBsConfig and returns it.
func NewPrimaryDBsConfig(dbs []*sqlx.DB, policy ReadWritePolicy) *PrimaryDBsConfig {
	return &PrimaryDBsConfig{
		DBs:             dbs,
		ReadWritePolicy: policy,
	}
}

// DBResolver chooses one of databases and then executes a query.
// This supposed to be aligned with sqlx.DB.
// Some functions which must select from multiple database are only available for the primary DBResolver
// or the first primary DBResolver (if using multi-primary). For example, `DriverName()`, `Unsafe()`.
type DBResolver interface {
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
	Beginx() (*sqlx.Tx, error)
	BindNamed(query string, arg interface{}) (string, []interface{}, error)
	Close() error
	Conn(ctx context.Context) (*sql.Conn, error)
	Connx(ctx context.Context) (*sqlx.Conn, error)
	Driver() driver.Driver
	DriverName() string
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	MapperFunc(mf func(string) string)
	MustBegin() *sqlx.Tx
	MustBeginTx(ctx context.Context, opts *sql.TxOptions) *sqlx.Tx
	MustExec(query string, args ...interface{}) sql.Result
	MustExecContext(ctx context.Context, query string, args ...interface{}) sql.Result
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error)
	Ping() error
	PingContext(ctx context.Context) error
	Prepare(query string) (Stmt, error)
	PrepareContext(ctx context.Context, query string) (Stmt, error)
	PrepareNamed(query string) (NamedStmt, error)
	PrepareNamedContext(ctx context.Context, query string) (NamedStmt, error)
	Preparex(query string) (Stmt, error)
	PreparexContext(ctx context.Context, query string) (Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	Rebind(query string) string
	Select(dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SetConnMaxIdleTime(d time.Duration)
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	Stats() sql.DBStats
	Unsafe() *sqlx.DB
}

type dbResolver struct {
	primaries   []*sqlx.DB
	secondaries []*sqlx.DB

	reads []*sqlx.DB

	loadBalancer LoadBalancer
}

var _ DBResolver = (*dbResolver)(nil)

// NewDBResolver creates a new DBResolver and returns it.
// If no primary DBResolver is given, it returns an error.
// If you do not give WriteOnly option, it will use the primary DBResolver as the read DBResolver.
// if you do not give LoadBalancer option, it will use the RandomLoadBalancer.
func NewDBResolver(primaryDBsCfg *PrimaryDBsConfig, opts ...OptionFunc) (DBResolver, error) {
	if primaryDBsCfg == nil || len(primaryDBsCfg.DBs) == 0 {
		return nil, errNoPrimaryDB
	}

	if primaryDBsCfg.ReadWritePolicy == "" {
		primaryDBsCfg.ReadWritePolicy = ReadWrite
	}
	if _, ok := validReadWritePolicies[primaryDBsCfg.ReadWritePolicy]; !ok {
		return nil, errInvalidReadWritePolicy
	}

	options, err := compileOptions(opts...)
	if err != nil {
		return nil, err
	}

	var reads []*sqlx.DB
	reads = append(reads, options.SecondaryDBs...)
	if primaryDBsCfg.ReadWritePolicy == ReadWrite {
		reads = append(reads, primaryDBsCfg.DBs...)
	}
	if len(reads) == 0 {
		return nil, errNoDBToRead
	}

	return &dbResolver{
		primaries:    primaryDBsCfg.DBs,
		secondaries:  options.SecondaryDBs,
		reads:        reads,
		loadBalancer: options.LoadBalancer,
	}, nil
}

func compileOptions(opts ...OptionFunc) (*Options, error) {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	if options.LoadBalancer == nil {
		options.LoadBalancer = NewRandomLoadBalancer()
	}

	return options, nil
}

func MustNewDBResolver(primaryDBsCfg *PrimaryDBsConfig, opts ...OptionFunc) DBResolver {
	db, err := NewDBResolver(primaryDBsCfg, opts...)
	if err != nil {
		panic(err)
	}
	return db
}

// Begin chooses a primary database and starts a transaction.
// This supposed to be aligned with sqlx.DB.Begin.
func (r *dbResolver) Begin() (*sql.Tx, error) {
	db := r.loadBalancer.Select(context.Background(), r.primaries)
	return db.Begin()
}

// BeginTx chooses a primary database and starts a transaction.
// This supposed to be aligned with sqlx.DB.BeginTx.
func (r *dbResolver) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	db := r.loadBalancer.Select(ctx, r.primaries)
	return db.BeginTx(ctx, opts)
}

// BeginTxx chooses a primary database, begins a transaction and returns an *sqlx.Tx.
// This supposed to be aligned with sqlx.DB.BeginTxx.
func (r *dbResolver) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	db := r.loadBalancer.Select(ctx, r.primaries)
	return db.BeginTxx(ctx, opts)
}

// Beginx chooses a primary database, begins a transaction and returns an *sqlx.Tx.
// This supposed to be aligned with sqlx.DB.Beginx.
func (r *dbResolver) Beginx() (*sqlx.Tx, error) {
	db := r.loadBalancer.Select(context.Background(), r.primaries)
	return db.Beginx()
}

// BindNamed chooses a primary database and binds a query using the DB driver's bindvar type.
// This supposed to be aligned with sqlx.DB.BindNamed.
func (r *dbResolver) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
	db := r.loadBalancer.Select(context.Background(), r.primaries)
	return db.BindNamed(query, arg)
}

// Close closes all the databases.
func (r *dbResolver) Close() error {
	var errs error
	for _, db := range r.primaries {
		if err := db.Close(); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	for _, db := range r.secondaries {
		if err := db.Close(); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	return errs
}

// Conn chooses a primary database and returns a *sql.Conn.
// This supposed to be aligned with sqlx.DB.Conn.
func (r *dbResolver) Conn(ctx context.Context) (*sql.Conn, error) {
	db := r.loadBalancer.Select(ctx, r.primaries)
	return db.Conn(ctx)
}

// Connx chooses a primary database and returns a *sqlx.Conn.
// This supposed to be aligned with sqlx.DB.Connx.
func (r *dbResolver) Connx(ctx context.Context) (*sqlx.Conn, error) {
	db := r.loadBalancer.Select(ctx, r.primaries)
	return db.Connx(ctx)
}

// Driver chooses a primary database and returns a driver.Driver.
// This supposed to be aligned with sqlx.DB.Driver.
func (r *dbResolver) Driver() driver.Driver {
	db := r.loadBalancer.Select(context.Background(), r.primaries)
	return db.Driver()
}

// DriverName chooses a primary database and returns the driverName.
// This supposed to be aligned with sqlx.DB.DriverName.
func (r *dbResolver) DriverName() string {
	db := r.loadBalancer.Select(context.Background(), r.primaries)
	return db.DriverName()
}

// Exec chooses a primary database and executes a query without returning any rows.
// This supposed to be aligned with sqlx.DB.Exec.
func (r *dbResolver) Exec(query string, args ...interface{}) (sql.Result, error) {
	db := r.loadBalancer.Select(context.Background(), r.primaries)
	return db.Exec(query, args...)
}

// ExecContext chooses a primary database and executes a query without returning any rows.
// This supposed to be aligned with sqlx.DB.ExecContext.
func (r *dbResolver) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	db := r.loadBalancer.Select(ctx, r.primaries)
	return db.Exec(query, args...)
}

// Get chooses a readable database and Get using chosen DB.
// This supposed to be aligned with sqlx.DB.Get.
func (r *dbResolver) Get(dest interface{}, query string, args ...interface{}) error {
	db := r.loadBalancer.Select(context.Background(), r.reads)
	err := db.Get(dest, query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(context.Background(), r.primaries)
		err = dbPrimary.Get(dest, query, args...)
	}
	return err
}

// GetContext chooses a readable database and Get using chosen DB.
// This supposed to be aligned with sqlx.DB.GetContext.
func (r *dbResolver) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	db := r.loadBalancer.Select(ctx, r.reads)
	err := db.GetContext(ctx, dest, query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(ctx, r.primaries)
		err = dbPrimary.GetContext(ctx, dest, query, args...)
	}
	return err
}

// MapperFunc sets the mapper function for the all primary databases and secondary databases.
func (r *dbResolver) MapperFunc(mf func(string) string) {
	for _, db := range r.primaries {
		db.MapperFunc(mf)
	}
	for _, db := range r.secondaries {
		db.MapperFunc(mf)
	}
}

// MustBegin chooses a primary database, starts a transaction and returns an *sqlx.Tx or panic.
// This supposed to be aligned with sqlx.DB.MustBegin.
func (r *dbResolver) MustBegin() *sqlx.Tx {
	db := r.loadBalancer.Select(context.Background(), r.primaries)
	return db.MustBegin()
}

// MustBeginTx chooses a primary database, starts a transaction and returns an *sqlx.Tx or panic.
// This supposed to be aligned with sqlx.DB.MustBeginTx.
func (r *dbResolver) MustBeginTx(ctx context.Context, opts *sql.TxOptions) *sqlx.Tx {
	db := r.loadBalancer.Select(ctx, r.primaries)
	return db.MustBeginTx(ctx, opts)
}

// MustExec chooses a primary database and executes a query or panic.
// This supposed to be aligned with sqlx.DB.MustExec.
func (r *dbResolver) MustExec(query string, args ...interface{}) sql.Result {
	db := r.loadBalancer.Select(context.Background(), r.primaries)
	return db.MustExec(query, args...)
}

// MustExecContext chooses a primary database and executes a query or panic.
// This supposed to be aligned with sqlx.DB.MustExecContext.
func (r *dbResolver) MustExecContext(ctx context.Context, query string, args ...interface{}) sql.Result {
	db := r.loadBalancer.Select(ctx, r.primaries)
	return db.MustExecContext(ctx, query, args...)
}

// NamedExec chooses a primary database and then executes a named query.
// This supposed to be aligned with sqlx.DB.NamedExec.
func (r *dbResolver) NamedExec(query string, arg interface{}) (sql.Result, error) {
	db := r.loadBalancer.Select(context.Background(), r.primaries)
	return db.NamedExec(query, arg)
}

// NamedExecContext chooses a primary database and then executes a named query.
// This supposed to be aligned with sqlx.DB.NamedExecContext.
func (r *dbResolver) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	db := r.loadBalancer.Select(ctx, r.primaries)
	return db.NamedExecContext(ctx, query, arg)
}

// NamedQuery chooses a readable database and then executes a named query.
// This supposed to be aligned with sqlx.DB.NamedQuery.
func (r *dbResolver) NamedQuery(query string, arg interface{}) (*sqlx.Rows, error) {
	db := r.loadBalancer.Select(context.Background(), r.reads)
	rows, err := db.NamedQuery(query, arg)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(context.Background(), r.primaries)
		rows, err = dbPrimary.NamedQuery(query, arg)
	}
	return rows, err
}

// NamedQueryContext chooses a readable database and then executes a named query.
// This supposed to be aligned with sqlx.DB.NamedQueryContext.
func (r *dbResolver) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	db := r.loadBalancer.Select(ctx, r.reads)
	rows, err := db.NamedQueryContext(ctx, query, arg)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(ctx, r.primaries)
		rows, err = dbPrimary.NamedQueryContext(ctx, query, arg)
	}
	return rows, err
}

// Ping sends a ping to the all databases.
func (r *dbResolver) Ping() error {
	var errs error
	for _, db := range r.primaries {
		if err := db.Ping(); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	for _, db := range r.secondaries {
		if err := db.Ping(); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	if errs != nil {
		return errs
	}
	return nil
}

// PingContext sends a ping to the all databases.
func (r *dbResolver) PingContext(ctx context.Context) error {
	var errs error
	for _, db := range r.primaries {
		if err := db.PingContext(ctx); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	for _, db := range r.secondaries {
		if err := db.PingContext(ctx); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	if errs != nil {
		return errs
	}
	return nil
}

// Prepare returns a Stmt which can be used sql.Stmt instead.
// This supposed to be aligned with sqlx.DB.Prepare.
func (r *dbResolver) Prepare(query string) (Stmt, error) {
	primaryDBStmts := make(map[*sqlx.DB]*sqlx.Stmt, len(r.primaries))
	readDBStmts := make(map[*sqlx.DB]*sqlx.Stmt, len(r.reads))

	var errs error
	for _, db := range r.primaries {
		stmt, err := db.Preparex(query)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, db := range r.reads {
		stmt, err := db.Preparex(query)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		readDBStmts[db] = stmt
	}
	if errs != nil {
		return nil, errs
	}

	return &stmt{
		primaries:    r.primaries,
		reads:        r.reads,
		primaryStmts: primaryDBStmts,
		readStmts:    readDBStmts,
		loadBalancer: r.loadBalancer,
	}, nil
}

// PrepareContext returns a Stmt which can be used sql.Stmt instead.
// This supposed to be aligned with sqlx.DB.PrepareContext.
func (r *dbResolver) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	primaryDBStmts := make(map[*sqlx.DB]*sqlx.Stmt, len(r.primaries))
	readDBStmts := make(map[*sqlx.DB]*sqlx.Stmt, len(r.reads))

	var errs error
	for _, db := range r.primaries {
		stmt, err := db.PreparexContext(ctx, query)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, db := range r.reads {
		stmt, err := db.PreparexContext(ctx, query)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		readDBStmts[db] = stmt
	}
	if errs != nil {
		return nil, errs
	}

	return &stmt{
		primaries:    r.primaries,
		reads:        r.reads,
		primaryStmts: primaryDBStmts,
		readStmts:    readDBStmts,
		loadBalancer: r.loadBalancer,
	}, nil
}

// PrepareNamed returns an NamedStmt which can be used sqlx.NamedStmt instead.
// This supposed to be aligned with sqlx.DB.PrepareNamed.
func (r *dbResolver) PrepareNamed(query string) (NamedStmt, error) {
	primaryDBStmts := make(map[*sqlx.DB]*sqlx.NamedStmt, len(r.primaries))
	readDBStmts := make(map[*sqlx.DB]*sqlx.NamedStmt, len(r.reads))

	var errs error
	for _, db := range r.primaries {
		stmt, err := db.PrepareNamed(query)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, db := range r.reads {
		stmt, err := db.PrepareNamed(query)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		readDBStmts[db] = stmt
	}
	if errs != nil {
		return nil, errs
	}

	return &namedStmt{
		primaries:    r.primaries,
		reads:        r.reads,
		primaryStmts: primaryDBStmts,
		readStmts:    readDBStmts,
		loadBalancer: r.loadBalancer,
	}, nil
}

// PrepareNamedContext returns an NamedStmt which can be used sqlx.NamedStmt instead.
// This supposed to be aligned with sqlx.DB.PrepareNamedContext.
func (r *dbResolver) PrepareNamedContext(ctx context.Context, query string) (NamedStmt, error) {
	primaryDBStmts := make(map[*sqlx.DB]*sqlx.NamedStmt, len(r.primaries))
	readDBStmts := make(map[*sqlx.DB]*sqlx.NamedStmt, len(r.reads))

	var errs error
	for _, db := range r.primaries {
		stmt, err := db.PrepareNamedContext(ctx, query)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, db := range r.reads {
		stmt, err := db.PrepareNamedContext(ctx, query)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		readDBStmts[db] = stmt
	}
	if errs != nil {
		return nil, errs
	}

	return &namedStmt{
		primaries:    r.primaries,
		reads:        r.reads,
		primaryStmts: primaryDBStmts,
		readStmts:    readDBStmts,
		loadBalancer: r.loadBalancer,
	}, nil
}

// Preparex returns an Stmt which can be used sqlx.Stmt instead.
// This supposed to be aligned with sqlx.DB.Preparex.
func (r *dbResolver) Preparex(query string) (Stmt, error) {
	primaryDBStmts := make(map[*sqlx.DB]*sqlx.Stmt, len(r.primaries))
	readDBStmts := make(map[*sqlx.DB]*sqlx.Stmt, len(r.reads))

	var errs error
	for _, db := range r.primaries {
		stmt, err := db.Preparex(query)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, db := range r.reads {
		stmt, err := db.Preparex(query)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		readDBStmts[db] = stmt
	}
	if errs != nil {
		return nil, errs
	}

	return &stmt{
		primaries:    r.primaries,
		reads:        r.reads,
		primaryStmts: primaryDBStmts,
		readStmts:    readDBStmts,
		loadBalancer: r.loadBalancer,
	}, nil
}

// PreparexContext returns a Stmt which can be used sqlx.Stmt instead.
// This supposed to be aligned with sqlx.DB.PreparexContext.
func (r *dbResolver) PreparexContext(ctx context.Context, query string) (Stmt, error) {
	primaryDBStmts := make(map[*sqlx.DB]*sqlx.Stmt, len(r.primaries))
	readDBStmts := make(map[*sqlx.DB]*sqlx.Stmt, len(r.reads))

	var errs error
	for _, db := range r.primaries {
		stmt, err := db.PreparexContext(ctx, query)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		primaryDBStmts[db] = stmt
	}
	for _, db := range r.reads {
		stmt, err := db.PreparexContext(ctx, query)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		readDBStmts[db] = stmt
	}
	if errs != nil {
		return nil, errs
	}

	return &stmt{
		primaries:    r.primaries,
		reads:        r.reads,
		primaryStmts: primaryDBStmts,
		readStmts:    readDBStmts,
		loadBalancer: r.loadBalancer,
	}, nil
}

// Query chooses a readable database, executes the query and executes a query that returns sql.Rows.
// This supposed to be aligned with sqlx.DB.Query.
func (r *dbResolver) Query(query string, args ...interface{}) (*sql.Rows, error) {
	db := r.loadBalancer.Select(context.Background(), r.reads)
	rows, err := db.Query(query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(context.Background(), r.primaries)
		rows, err = dbPrimary.Query(query, args...)
	}
	return rows, err
}

// QueryContext chooses a readable database, executes the query and executes a query that returns sql.Rows.
// This supposed to be aligned with sqlx.DB.QueryContext.
func (r *dbResolver) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	db := r.loadBalancer.Select(ctx, r.reads)
	rows, err := db.QueryContext(ctx, query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(ctx, r.primaries)
		rows, err = dbPrimary.QueryContext(ctx, query, args...)
	}
	return rows, err
}

// QueryRow chooses a readable database, executes the query and executes a query that returns sql.Row.
// This supposed to be aligned with sqlx.DB.QueryRow.
func (r *dbResolver) QueryRow(query string, args ...interface{}) *sql.Row {
	db := r.loadBalancer.Select(context.Background(), r.reads)
	row := db.QueryRow(query, args...)
	if isDBConnectionError(row.Err()) {
		dbPrimary := r.loadBalancer.Select(context.Background(), r.primaries)
		row = dbPrimary.QueryRow(query, args...)
	}
	return row
}

// QueryRowContext chooses a readable database, executes the query and executes a query that returns sql.Row.
// This supposed to be aligned with sqlx.DB.QueryRowContext.
func (r *dbResolver) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	db := r.loadBalancer.Select(ctx, r.reads)
	row := db.QueryRowContext(ctx, query, args...)
	if isDBConnectionError(row.Err()) {
		dbPrimary := r.loadBalancer.Select(ctx, r.primaries)
		row = dbPrimary.QueryRowContext(ctx, query, args...)
	}
	return row
}

// QueryRowx chooses a readable database, queries the database and returns an *sqlx.Row.
// This supposed to be aligned with sqlx.DB.QueryRowx.
func (r *dbResolver) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	db := r.loadBalancer.Select(context.Background(), r.reads)
	row := db.QueryRowx(query, args...)
	if isDBConnectionError(row.Err()) {
		dbPrimary := r.loadBalancer.Select(context.Background(), r.primaries)
		row = dbPrimary.QueryRowx(query, args...)
	}
	return row
}

// QueryRowxContext chooses a readable database, queries the database and returns an *sqlx.Row.
// This supposed to be aligned with sqlx.DB.QueryRowxContext.
func (r *dbResolver) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	db := r.loadBalancer.Select(ctx, r.reads)
	row := db.QueryRowxContext(ctx, query, args...)
	if isDBConnectionError(row.Err()) {
		dbPrimary := r.loadBalancer.Select(ctx, r.primaries)
		row = dbPrimary.QueryRowxContext(ctx, query, args...)
	}
	return row
}

// Queryx chooses a readable database, queries the database and returns an *sqlx.Rows.
// This supposed to be aligned with sqlx.DB.Queryx.
func (r *dbResolver) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	db := r.loadBalancer.Select(context.Background(), r.reads)
	rows, err := db.Queryx(query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(context.Background(), r.primaries)
		rows, err = dbPrimary.Queryx(query, args...)
	}
	return rows, err
}

// QueryxContext chooses a readable database, queries the database and returns an *sqlx.Rows.
// This supposed to be aligned with sqlx.DB.QueryxContext.
func (r *dbResolver) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	db := r.loadBalancer.Select(ctx, r.reads)
	rows, err := db.QueryxContext(ctx, query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(ctx, r.primaries)
		rows, err = dbPrimary.QueryxContext(ctx, query, args...)
	}
	return rows, err
}

// Rebind chooses a primary database and
// transforms a query from QUESTION to the DB driver's bindvar type.
// This supposed to be aligned with sqlx.DB.Rebind.
func (r *dbResolver) Rebind(query string) string {
	db := r.loadBalancer.Select(context.Background(), r.primaries)
	return db.Rebind(query)
}

// Select chooses a readable database and execute SELECT using chosen DB.
// This supposed to be aligned with sqlx.DB.Select.
func (r *dbResolver) Select(dest interface{}, query string, args ...interface{}) error {
	db := r.loadBalancer.Select(context.Background(), r.reads)
	err := db.Select(dest, query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(context.Background(), r.primaries)
		err = dbPrimary.Select(dest, query, args...)
	}
	return err
}

// SelectContext chooses a readable database and execute SELECT using chosen DB.
// This supposed to be aligned with sqlx.DB.SelectContext.
func (r *dbResolver) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	db := r.loadBalancer.Select(ctx, r.reads)
	err := db.SelectContext(ctx, dest, query, args...)
	if isDBConnectionError(err) {
		dbPrimary := r.loadBalancer.Select(ctx, r.primaries)
		err = dbPrimary.SelectContext(ctx, dest, query, args...)
	}
	return err
}

// SetConnMaxIdleTime sets the maximum amount of time a connection may be idle to all databases.
func (r *dbResolver) SetConnMaxIdleTime(d time.Duration) {
	for _, db := range r.primaries {
		db.SetConnMaxIdleTime(d)
	}
	for _, db := range r.reads {
		db.SetConnMaxIdleTime(d)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused to all databases.
func (r *dbResolver) SetConnMaxLifetime(d time.Duration) {
	for _, db := range r.primaries {
		db.SetConnMaxLifetime(d)
	}
	for _, db := range r.reads {
		db.SetConnMaxLifetime(d)
	}
}

// SetMaxIdleConns sets the maximum number of connections in the idle connection pool to all databases.
func (r *dbResolver) SetMaxIdleConns(n int) {
	for _, db := range r.primaries {
		db.SetMaxIdleConns(n)
	}
	for _, db := range r.reads {
		db.SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections to all databases.
func (r *dbResolver) SetMaxOpenConns(n int) {
	for _, db := range r.primaries {
		db.SetMaxOpenConns(n)
	}
	for _, db := range r.reads {
		db.SetMaxOpenConns(n)
	}
}

// Stats returns first primary database statistics.
func (r *dbResolver) Stats() sql.DBStats {
	return r.primaries[0].Stats()
}

// Unsafe chose a primary database and returns a version of DB
// which will silently succeed to scan
// when columns in the SQL result have no fields in the destination struct.
// This supposed to be aligned with sqlx.DB.Unsafe.
func (r *dbResolver) Unsafe() *sqlx.DB {
	db := r.loadBalancer.Select(context.Background(), r.primaries)
	return db.Unsafe()
}
