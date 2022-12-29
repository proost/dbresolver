package dbresolver

import (
	"context"
	"database/sql"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
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
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
	Beginx() (*sqlx.Tx, error)
	BindNamed(query string, arg interface{}) (string, []interface{}, error)
	Connx(ctx context.Context) (*sqlx.Conn, error)
	DriverName() string
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
	PrepareNamed(query string) (NamedStmt, error)
	PrepareNamedContext(ctx context.Context, query string) (NamedStmt, error)
	Preparex(query string) (Stmt, error)
	PreparexContext(ctx context.Context, query string) (Stmt, error)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	Rebind(query string) string
	Select(dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
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

// Connx chooses a primary database and returns an *sqlx.Conn.
// This supposed to be aligned with sqlx.DB.Connx.
func (r *dbResolver) Connx(ctx context.Context) (*sqlx.Conn, error) {
	db := r.loadBalancer.Select(ctx, r.primaries)
	return db.Connx(ctx)
}

// DriverName chooses a primary database and returns the driverName.
// This supposed to be aligned with sqlx.DB.DriverName.
func (r *dbResolver) DriverName() string {
	db := r.loadBalancer.Select(context.Background(), r.primaries)
	return db.DriverName()
}

// Get chooses a readable database and Get using chosen DB.
// This supposed to be aligned with sqlx.DB.Get.
func (r *dbResolver) Get(dest interface{}, query string, args ...interface{}) error {
	db := r.loadBalancer.Select(context.Background(), r.reads)
	return db.Get(dest, query, args...)
}

// GetContext chooses a readable database and Get using chosen DB.
// This supposed to be aligned with sqlx.DB.GetContext.
func (r *dbResolver) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	db := r.loadBalancer.Select(ctx, r.reads)
	return db.GetContext(ctx, dest, query, args...)
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
	return db.NamedQuery(query, arg)
}

// NamedQueryContext chooses a readable database and then executes a named query.
// This supposed to be aligned with sqlx.DB.NamedQueryContext.
func (r *dbResolver) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	db := r.loadBalancer.Select(ctx, r.reads)
	return db.NamedQueryContext(ctx, query, arg)
}

// PrepareNamed returns an NamedStmt which can be used sqlx.NamedStmt instead.
// This supposed to be aligned with sqlx.DB.PrepareNamed.
func (r *dbResolver) PrepareNamed(query string) (NamedStmt, error) {
	primaryDBStmts := make(map[*sqlx.DB]*sqlx.NamedStmt, len(r.primaries))
	readDBStmts := make(map[*sqlx.DB]*sqlx.NamedStmt, len(r.reads))

	var mu sync.Mutex
	g, _ := errgroup.WithContext(context.Background())
	for _, db := range r.primaries {
		db := db
		g.Go(func() error {
			stmt, err := db.PrepareNamed(query)
			if err != nil {
				return err
			}

			mu.Lock()
			primaryDBStmts[db] = stmt
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	for _, db := range r.reads {
		db := db
		g.Go(func() error {
			stmt, err := db.PrepareNamed(query)
			if err != nil {
				return err
			}

			mu.Lock()
			readDBStmts[db] = stmt
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
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

	var mu sync.Mutex
	g, gCtx := errgroup.WithContext(ctx)
	for _, db := range r.primaries {
		db := db
		g.Go(func() error {
			stmt, err := db.PrepareNamedContext(gCtx, query)
			if err != nil {
				return err
			}

			mu.Lock()
			primaryDBStmts[db] = stmt
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	g, gCtx = errgroup.WithContext(ctx)
	for _, db := range r.reads {
		db := db
		g.Go(func() error {
			stmt, err := db.PrepareNamedContext(gCtx, query)
			if err != nil {
				return err
			}

			mu.Lock()
			readDBStmts[db] = stmt
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &namedStmt{
		primaries:    r.primaries,
		reads:        r.reads,
		primaryStmts: primaryDBStmts,
		readStmts:    readDBStmts,
		loadBalancer: r.loadBalancer,
	}, nil
}

// Preparex returns an sqlx.Stmt which can be used sqlx.Stmt instead.
// This supposed to be aligned with sqlx.DB.Preparex.
func (r *dbResolver) Preparex(query string) (Stmt, error) {
	primaryDBStmts := make(map[*sqlx.DB]*sqlx.Stmt, len(r.primaries))
	readDBStmts := make(map[*sqlx.DB]*sqlx.Stmt, len(r.reads))

	var mu sync.Mutex
	g, _ := errgroup.WithContext(context.Background())
	for _, db := range r.primaries {
		db := db
		g.Go(func() error {
			stmt, err := db.Preparex(query)
			if err != nil {
				return err
			}

			mu.Lock()
			primaryDBStmts[db] = stmt
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	for _, db := range r.reads {
		db := db
		g.Go(func() error {
			stmt, err := db.Preparex(query)
			if err != nil {
				return err
			}

			mu.Lock()
			readDBStmts[db] = stmt
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &stmt{
		primaries:    r.primaries,
		reads:        r.reads,
		primaryStmts: primaryDBStmts,
		readStmts:    readDBStmts,
		loadBalancer: r.loadBalancer,
	}, nil
}

// PreparexContext returns a sqlx.Stmt which can be used sqlx.Stmt instead.
// This supposed to be aligned with sqlx.DB.PreparexContext.
func (r *dbResolver) PreparexContext(ctx context.Context, query string) (Stmt, error) {
	primaryDBStmts := make(map[*sqlx.DB]*sqlx.Stmt, len(r.primaries))
	readDBStmts := make(map[*sqlx.DB]*sqlx.Stmt, len(r.reads))

	var mu sync.Mutex
	g, gCtx := errgroup.WithContext(ctx)
	for _, db := range r.primaries {
		db := db
		g.Go(func() error {
			stmt, err := db.PreparexContext(gCtx, query)
			if err != nil {
				return err
			}

			mu.Lock()
			primaryDBStmts[db] = stmt
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	g, gCtx = errgroup.WithContext(ctx)
	for _, db := range r.reads {
		db := db
		g.Go(func() error {
			stmt, err := db.PreparexContext(gCtx, query)
			if err != nil {
				return err
			}

			mu.Lock()
			readDBStmts[db] = stmt
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &stmt{
		primaries:    r.primaries,
		reads:        r.reads,
		primaryStmts: primaryDBStmts,
		readStmts:    readDBStmts,
		loadBalancer: r.loadBalancer,
	}, nil
}

// QueryRowx chooses a readable database, queries the database and returns an *sqlx.Row.
// This supposed to be aligned with sqlx.DB.QueryRowx.
func (r *dbResolver) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	db := r.loadBalancer.Select(context.Background(), r.reads)
	return db.QueryRowx(query, args...)
}

// QueryRowxContext chooses a readable database, queries the database and returns an *sqlx.Row.
// This supposed to be aligned with sqlx.DB.QueryRowxContext.
func (r *dbResolver) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	db := r.loadBalancer.Select(ctx, r.reads)
	return db.QueryRowxContext(ctx, query, args...)
}

// Queryx chooses a readable database, queries the database and returns an *sqlx.Rows.
// This supposed to be aligned with sqlx.DB.Queryx.
func (r *dbResolver) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	db := r.loadBalancer.Select(context.Background(), r.reads)
	return db.Queryx(query, args...)
}

// QueryxContext chooses a readable database, queries the database and returns an *sqlx.Rows.
// This supposed to be aligned with sqlx.DB.QueryxContext.
func (r *dbResolver) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	db := r.loadBalancer.Select(ctx, r.reads)
	return db.QueryxContext(ctx, query, args...)
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
	return db.Select(dest, query, args...)
}

// SelectContext chooses a readable database and execute SELECT using chosen DB.
// This supposed to be aligned with sqlx.DB.SelectContext.
func (r *dbResolver) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	db := r.loadBalancer.Select(ctx, r.reads)
	return db.SelectContext(ctx, dest, query, args...)
}

// Unsafe chose a primary database and returns a version of DB
// which will silently succeed to scan
// when columns in the SQL result have no fields in the destination struct.
// This supposed to be aligned with sqlx.DB.Unsafe.
func (r *dbResolver) Unsafe() *sqlx.DB {
	db := r.loadBalancer.Select(context.Background(), r.primaries)
	return db.Unsafe()
}
