package dbresolver

import (
	"context"
	"errors"
	"math/rand"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestNewDBResolver(t *testing.T) {
	t.Run("without primary dbs config", func(t *testing.T) {
		result, err := NewDBResolver(nil)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, errNoPrimaryDB)
	})

	t.Run("without primary dbs", func(t *testing.T) {
		result, err := NewDBResolver(&PrimaryDBsConfig{})

		assert.Nil(t, result)
		assert.ErrorIs(t, err, errNoPrimaryDB)
	})

	t.Run("with primary db & no read-write policy option", func(t *testing.T) {
		mockDB, _, err := sqlmock.New()
		assert.NoError(t, err)
		mockPrimaryDB := sqlx.NewDb(mockDB, "primary")
		primaryDBsConfig := &PrimaryDBsConfig{
			DBs: []*sqlx.DB{mockPrimaryDB},
		}

		result, err := NewDBResolver(primaryDBsConfig)

		assert.NoError(t, err)
		expected := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB},
			reads:     []*sqlx.DB{mockPrimaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: random,
			},
		}
		assert.Equal(t, expected, result)
	})

	t.Run("with primary db & invalid read-write policy option", func(t *testing.T) {
		mockDB, _, err := sqlmock.New()
		assert.NoError(t, err)
		mockPrimaryDB := sqlx.NewDb(mockDB, "primary")
		primaryDBsConfig := &PrimaryDBsConfig{
			DBs:             []*sqlx.DB{mockPrimaryDB},
			ReadWritePolicy: "invalid",
		}

		result, err := NewDBResolver(primaryDBsConfig)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, errInvalidReadWritePolicy)
	})

	t.Run("with only write-only primary db", func(t *testing.T) {
		mockDB, _, err := sqlmock.New()
		assert.NoError(t, err)
		mockPrimaryDB := sqlx.NewDb(mockDB, "primary")
		primaryDBsConfig := &PrimaryDBsConfig{
			DBs:             []*sqlx.DB{mockPrimaryDB},
			ReadWritePolicy: WriteOnly,
		}

		result, err := NewDBResolver(primaryDBsConfig)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, errNoDBToRead)
	})

	t.Run("with secondary db", func(t *testing.T) {
		mockDB, _, err := sqlmock.New()
		assert.NoError(t, err)
		mockPrimaryDB := sqlx.NewDb(mockDB, "primary")
		mockSecondaryDB := sqlx.NewDb(mockDB, "secondary")
		primaryDBsConfig := &PrimaryDBsConfig{
			DBs:             []*sqlx.DB{mockPrimaryDB},
			ReadWritePolicy: ReadWrite,
		}

		result, err := NewDBResolver(
			primaryDBsConfig,
			WithSecondaryDBs(mockSecondaryDB),
		)

		assert.NoError(t, err)
		expected := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: random,
			},
			reads: []*sqlx.DB{mockSecondaryDB, mockPrimaryDB},
		}
		assert.Equal(t, expected, result)
	})

	t.Run("with write-only primary db & secondary", func(t *testing.T) {
		mockDB, _, err := sqlmock.New()
		assert.NoError(t, err)
		mockPrimaryDB := sqlx.NewDb(mockDB, "primary")
		mockSecondaryDB := sqlx.NewDb(mockDB, "secondary")
		primaryDBsConfig := &PrimaryDBsConfig{
			DBs:             []*sqlx.DB{mockPrimaryDB},
			ReadWritePolicy: WriteOnly,
		}

		result, err := NewDBResolver(
			primaryDBsConfig,
			WithSecondaryDBs(mockSecondaryDB),
		)

		assert.NoError(t, err)
		expected := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: random,
			},
			reads: []*sqlx.DB{mockSecondaryDB},
		}
		assert.Equal(t, expected, result)
	})
}

func TestDBResolver_BeginTxx(t *testing.T) {
	t.Run("return error", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New()
		mockError := errors.New("mock error")
		sqlMock.ExpectBegin().
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakePrimaryDB, _, _ := sqlmock.New()
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB, sqlx.NewDb(fakePrimaryDB, "fake")},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result, err := r.BeginTxx(context.Background(), nil)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New()
		sqlMock.ExpectBegin()
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakePrimaryDB, _, _ := sqlmock.New()
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB, sqlx.NewDb(fakePrimaryDB, "fake")},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result, err := r.BeginTxx(context.Background(), nil)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &sqlx.Tx{}, result)
	})
}

func TestDBResolver_Beginx(t *testing.T) {
	t.Run("return error", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New()
		mockError := errors.New("mock error")
		sqlMock.ExpectBegin().
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakePrimaryDB, _, _ := sqlmock.New()
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB, sqlx.NewDb(fakePrimaryDB, "fake")},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result, err := r.Beginx()

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New()
		sqlMock.ExpectBegin()
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakePrimaryDB, _, _ := sqlmock.New()
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB, sqlx.NewDb(fakePrimaryDB, "fake")},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result, err := r.Beginx()

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.IsType(t, &sqlx.Tx{}, result)
	})
}

func TestDBResolver_BindNamed(t *testing.T) {
	t.Run("return error", func(t *testing.T) {
		mockDB, _, _ := sqlmock.New()
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		r := &dbResolver{
			primaries:    []*sqlx.DB{mockPrimaryDB},
			loadBalancer: NewRandomLoadBalancer(),
		}

		result, args, err := r.BindNamed("SELECT :first_name", map[string]string{"first_name": "foo"})

		assert.Equal(t, "", result)
		assert.Nil(t, args)
		assert.ErrorContains(t, err, "unsupported map type")
	})

	t.Run("success with dollar bindvar type", func(t *testing.T) {
		mockDB, _, _ := sqlmock.New()
		mockPrimaryDB := sqlx.NewDb(mockDB, "postgres")
		r := &dbResolver{
			primaries:    []*sqlx.DB{mockPrimaryDB},
			loadBalancer: NewRandomLoadBalancer(),
		}

		result, args, err := r.BindNamed("SELECT :first_name", map[string]interface{}{"first_name": "foo"})

		assert.NoError(t, err)
		assert.Equal(t, "SELECT $1", result)
		assert.Equal(t, []interface{}{"foo"}, args)
	})

	t.Run("success with question binavar type", func(t *testing.T) {
		mockDB, _, _ := sqlmock.New()
		mockPrimaryDB := sqlx.NewDb(mockDB, "mysql")
		r := &dbResolver{
			primaries:    []*sqlx.DB{mockPrimaryDB},
			loadBalancer: NewRandomLoadBalancer(),
		}

		result, args, err := r.BindNamed("SELECT :first_name", map[string]interface{}{"first_name": "foo"})

		assert.NoError(t, err)
		assert.Equal(t, "SELECT ?", result)
		assert.Equal(t, []interface{}{"foo"}, args)
	})

	t.Run("success with named binavar type", func(t *testing.T) {
		mockDB, _, _ := sqlmock.New()
		mockPrimaryDB := sqlx.NewDb(mockDB, "ora")
		r := &dbResolver{
			primaries:    []*sqlx.DB{mockPrimaryDB},
			loadBalancer: NewRandomLoadBalancer(),
		}

		result, args, err := r.BindNamed("SELECT :first_name", map[string]interface{}{"first_name": "foo"})

		assert.NoError(t, err)
		assert.Equal(t, "SELECT :first_name", result)
		assert.Equal(t, []interface{}{"foo"}, args)
	})

	t.Run("success with at binavar type", func(t *testing.T) {
		mockDB, _, _ := sqlmock.New()
		mockPrimaryDB := sqlx.NewDb(mockDB, "sqlserver")
		r := &dbResolver{
			primaries:    []*sqlx.DB{mockPrimaryDB},
			loadBalancer: NewRandomLoadBalancer(),
		}

		result, args, err := r.BindNamed("SELECT :first_name", map[string]interface{}{"first_name": "foo"})

		assert.NoError(t, err)
		assert.Equal(t, "SELECT @p1", result)
		assert.Equal(t, []interface{}{"foo"}, args)
	})
}

func TestDBResolver_DriverName(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New()
		mockError := errors.New("mock error")
		sqlMock.ExpectBegin().
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakePrimaryDB, _, _ := sqlmock.New()
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB, sqlx.NewDb(fakePrimaryDB, "fake")},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := r.DriverName()

		assert.Equal(t, "mock", result)
	})
}

func TestDBResolver_Get(t *testing.T) {
	t.Run("return error", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		result := &Person{}
		err := r.Get(result, `SELECT * FROM person WHERE first_name=?`, "foo")

		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnRows(sqlmock.NewRows([]string{"first_name", "last_name"}).AddRow("foo", "bar"))
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		result := &Person{}
		err := r.Get(result, `SELECT * FROM person WHERE first_name=?`, "foo")

		assert.NoError(t, err)
		expected := &Person{
			FirstName: "foo",
			LastName:  "bar",
		}
		assert.Equal(t, expected, result)
	})
}

func TestDBResolver_GetContext(t *testing.T) {
	t.Run("return error", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		result := &Person{}
		err := r.GetContext(context.Background(), result, `SELECT * FROM person WHERE first_name=?`, "foo")

		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnRows(sqlmock.NewRows([]string{"first_name", "last_name"}).AddRow("foo", "bar"))
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		result := &Person{}
		err := r.GetContext(context.Background(), result, `SELECT * FROM person WHERE first_name=?`, "foo")

		assert.NoError(t, err)
		expected := &Person{
			FirstName: "foo",
			LastName:  "bar",
		}
		assert.Equal(t, expected, result)
	})
}

func TestDBResolver_MustBegin(t *testing.T) {
	t.Run("panic", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New()
		mockError := errors.New("mock error")
		sqlMock.ExpectBegin().
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakePrimaryDB, _, _ := sqlmock.New()
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB, sqlx.NewDb(fakePrimaryDB, "fake")},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		assert.Panics(t, func() {
			result := r.MustBegin()

			assert.Nil(t, result)
		})
	})

	t.Run("success", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New()
		sqlMock.ExpectBegin()
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakePrimaryDB, _, _ := sqlmock.New()
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB, sqlx.NewDb(fakePrimaryDB, "fake")},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := r.MustBegin()

		assert.IsType(t, &sqlx.Tx{}, result)
	})
}

func TestDBResolver_MustBeginTx(t *testing.T) {
	t.Run("panic", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New()
		mockError := errors.New("mock error")
		sqlMock.ExpectBegin().
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakePrimaryDB, _, _ := sqlmock.New()
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB, sqlx.NewDb(fakePrimaryDB, "fake")},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		assert.Panics(t, func() {
			result := r.MustBeginTx(context.Background(), nil)

			assert.Nil(t, result)
		})
	})

	t.Run("success", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New()
		sqlMock.ExpectBegin()
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakePrimaryDB, _, _ := sqlmock.New()
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB, sqlx.NewDb(fakePrimaryDB, "fake")},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := r.MustBeginTx(context.Background(), nil)

		assert.IsType(t, &sqlx.Tx{}, result)
	})
}

func TestDBResolver_MustExec(t *testing.T) {
	t.Run("panic", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectExec(`INSERT INTO person (first_name, last_name) VALUES ("foo", "bar")`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		assert.Panics(t, func() {
			result := r.MustExec(`INSERT INTO person (first_name, last_name) VALUES ("foo", "bar")`)

			assert.Nil(t, result)
		})
	})

	t.Run("success", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockResult := sqlmock.NewResult(1, 1)
		sqlMock.ExpectExec(`INSERT INTO person (first_name, last_name) VALUES ("foo", "bar")`).
			WillReturnResult(mockResult)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		result := r.MustExec(`INSERT INTO person (first_name, last_name) VALUES ("foo", "bar")`)

		lastInsertIDResult, err := result.LastInsertId()
		assert.NoError(t, err)
		lastRowsAffected, err := result.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), lastInsertIDResult)
		assert.Equal(t, int64(1), lastRowsAffected)
	})
}

func TestDBResolver_MustExecContext(t *testing.T) {
	t.Run("panic", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectExec(`INSERT INTO person (first_name, last_name) VALUES ("foo", "bar")`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		assert.Panics(t, func() {
			result := r.MustExecContext(context.Background(), `INSERT INTO person (first_name, last_name) VALUES ("foo", "bar")`)

			assert.Nil(t, result)
		})
	})

	t.Run("success", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockResult := sqlmock.NewResult(1, 1)
		sqlMock.ExpectExec(`INSERT INTO person (first_name, last_name) VALUES ("foo", "bar")`).
			WillReturnResult(mockResult)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		result := r.MustExecContext(context.Background(), `INSERT INTO person (first_name, last_name) VALUES ("foo", "bar")`)

		lastInsertIDResult, err := result.LastInsertId()
		assert.NoError(t, err)
		lastRowsAffected, err := result.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), lastInsertIDResult)
		assert.Equal(t, int64(1), lastRowsAffected)
	})
}

func TestDBResolver_NamedExec(t *testing.T) {
	t.Run("return error", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectExec(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		inputQuery := `INSERT INTO person (first_name, last_name) VALUES (:firstName, :lastName)`
		inputArgs := map[string]interface{}{
			"firstName": "foo",
			"lastName":  "bar",
		}
		result, err := r.NamedExec(inputQuery, inputArgs)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockResult := sqlmock.NewResult(1, 1)
		sqlMock.ExpectExec(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			WillReturnResult(mockResult)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		inputQuery := `INSERT INTO person (first_name, last_name) VALUES (:firstName, :lastName)`
		inputArgs := map[string]interface{}{
			"firstName": "foo",
			"lastName":  "bar",
		}
		result, err := r.NamedExec(inputQuery, inputArgs)

		assert.NoError(t, err)
		lastInsertIDResult, err := result.LastInsertId()
		assert.NoError(t, err)
		lastRowsAffected, err := result.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), lastInsertIDResult)
		assert.Equal(t, int64(1), lastRowsAffected)
	})
}

func TestDBResolver_NamedExecContext(t *testing.T) {
	t.Run("return error", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectExec(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		inputQuery := `INSERT INTO person (first_name, last_name) VALUES (:firstName, :lastName)`
		inputArgs := map[string]interface{}{
			"firstName": "foo",
			"lastName":  "bar",
		}
		result, err := r.NamedExecContext(context.Background(), inputQuery, inputArgs)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockResult := sqlmock.NewResult(1, 1)
		sqlMock.ExpectExec(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			WillReturnResult(mockResult)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		inputQuery := `INSERT INTO person (first_name, last_name) VALUES (:firstName, :lastName)`
		inputArgs := map[string]interface{}{
			"firstName": "foo",
			"lastName":  "bar",
		}
		result, err := r.NamedExecContext(context.Background(), inputQuery, inputArgs)

		assert.NoError(t, err)
		lastInsertIDResult, err := result.LastInsertId()
		assert.NoError(t, err)
		lastRowsAffected, err := result.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), lastInsertIDResult)
		assert.Equal(t, int64(1), lastRowsAffected)
	})
}

func TestDBResolver_NamedQuery(t *testing.T) {
	t.Run("return error", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=:firstName`
		inputArgs := map[string]interface{}{
			"firstName": "foo",
		}
		result, err := r.NamedQuery(inputQuery, inputArgs)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnRows(
				sqlmock.NewRows([]string{"first_name", "last_name"}).
					AddRow("foo", "bar").
					AddRow("foo", "baz"),
			)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=:firstName`
		inputArgs := map[string]interface{}{
			"firstName": "foo",
		}
		result, err := r.NamedQuery(inputQuery, inputArgs)

		assert.NoError(t, err)

		expected := []*Person{
			{
				FirstName: "foo",
				LastName:  "bar",
			},
			{
				FirstName: "foo",
				LastName:  "baz",
			},
		}
		i := 0
		for result.Next() {
			var person Person
			err := result.StructScan(&person)
			assert.NoError(t, err)

			assert.Equal(t, expected[i], &person)
			i++
		}
	})
}

func TestDBResolver_NamedQueryContext(t *testing.T) {
	t.Run("return error", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=:firstName`
		inputArgs := map[string]interface{}{
			"firstName": "foo",
		}
		result, err := r.NamedQueryContext(context.Background(), inputQuery, inputArgs)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnRows(
				sqlmock.NewRows([]string{"first_name", "last_name"}).
					AddRow("foo", "bar").
					AddRow("foo", "baz"),
			)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=:firstName`
		inputArgs := map[string]interface{}{
			"firstName": "foo",
		}
		result, err := r.NamedQueryContext(context.Background(), inputQuery, inputArgs)

		assert.NoError(t, err)

		expected := []*Person{
			{
				FirstName: "foo",
				LastName:  "bar",
			},
			{
				FirstName: "foo",
				LastName:  "baz",
			},
		}
		i := 0
		for result.Next() {
			var person Person
			err := result.StructScan(&person)
			assert.NoError(t, err)

			assert.Equal(t, expected[i], &person)
			i++
		}
	})
}

func TestDBResolver_PrepareNamed(t *testing.T) {
	t.Run("failed to prepare primary DB named statement", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=:firstName`
		result, err := r.PrepareNamed(inputQuery)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("failed to prepare readable DB named statement", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockError := errors.New("mock error")
		sqlMock.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockSecondaryDB := sqlx.NewDb(mockDB, "mock")
		r := &dbResolver{
			primaries: []*sqlx.DB{sqlx.NewDb(mockDB, "mock")},
			reads:     []*sqlx.DB{mockSecondaryDB},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=:firstName`
		result, err := r.PrepareNamed(inputQuery)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockDB3, sqlMock3, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock3.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockReadDB1 := sqlx.NewDb(mockDB3, "mock")
		mockDB4, sqlMock4, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock4.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockReadDB2 := sqlx.NewDb(mockDB4, "mock")
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			reads:     []*sqlx.DB{mockReadDB1, mockReadDB2},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=:firstName`
		result, err := r.PrepareNamed(inputQuery)

		resultNamedStmt := result.(*namedStmt)
		assert.NoError(t, err)
		assert.Equal(t, []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2}, resultNamedStmt.primaries)
		assert.Equal(t, []*sqlx.DB{mockReadDB1, mockReadDB2}, resultNamedStmt.reads)
		assert.IsType(t, &sqlx.NamedStmt{}, resultNamedStmt.primaryStmts[mockPrimaryDB1])
		assert.IsType(t, &sqlx.NamedStmt{}, resultNamedStmt.primaryStmts[mockPrimaryDB2])
		assert.IsType(t, &sqlx.NamedStmt{}, resultNamedStmt.readStmts[mockReadDB1])
		assert.IsType(t, &sqlx.NamedStmt{}, resultNamedStmt.readStmts[mockReadDB2])
	})
}

func TestDBResolver_PrepareNamedContext(t *testing.T) {
	t.Run("failed to prepare primary DB named statement", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=:firstName`
		result, err := r.PrepareNamedContext(context.Background(), inputQuery)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("failed to prepare readable DB named statement", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockError := errors.New("mock error")
		sqlMock.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockSecondaryDB := sqlx.NewDb(mockDB, "mock")
		r := &dbResolver{
			primaries: []*sqlx.DB{sqlx.NewDb(mockDB, "mock")},
			reads:     []*sqlx.DB{mockSecondaryDB},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=:firstName`
		result, err := r.PrepareNamedContext(context.Background(), inputQuery)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockDB3, sqlMock3, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock3.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockReadDB1 := sqlx.NewDb(mockDB3, "mock")
		mockDB4, sqlMock4, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock4.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockReadDB2 := sqlx.NewDb(mockDB4, "mock")
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			reads:     []*sqlx.DB{mockReadDB1, mockReadDB2},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=:firstName`
		result, err := r.PrepareNamedContext(context.Background(), inputQuery)

		resultNamedStmt := result.(*namedStmt)
		assert.NoError(t, err)
		assert.Equal(t, []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2}, resultNamedStmt.primaries)
		assert.Equal(t, []*sqlx.DB{mockReadDB1, mockReadDB2}, resultNamedStmt.reads)
		assert.IsType(t, &sqlx.NamedStmt{}, resultNamedStmt.primaryStmts[mockPrimaryDB1])
		assert.IsType(t, &sqlx.NamedStmt{}, resultNamedStmt.primaryStmts[mockPrimaryDB2])
		assert.IsType(t, &sqlx.NamedStmt{}, resultNamedStmt.readStmts[mockReadDB1])
		assert.IsType(t, &sqlx.NamedStmt{}, resultNamedStmt.readStmts[mockReadDB2])
	})
}

func TestDBResolver_Preparex(t *testing.T) {
	t.Run("failed to prepare primary DB statement", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=?`
		result, err := r.Preparex(inputQuery)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("failed to prepare readable DB statement", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockError := errors.New("mock error")
		sqlMock.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockSecondaryDB := sqlx.NewDb(mockDB, "mock")
		r := &dbResolver{
			primaries: []*sqlx.DB{sqlx.NewDb(mockDB, "mock")},
			reads:     []*sqlx.DB{mockSecondaryDB},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=?`
		result, err := r.Preparex(inputQuery)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockDB3, sqlMock3, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock3.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockReadDB1 := sqlx.NewDb(mockDB3, "mock")
		mockDB4, sqlMock4, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock4.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockReadDB2 := sqlx.NewDb(mockDB4, "mock")
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			reads:     []*sqlx.DB{mockReadDB1, mockReadDB2},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=?`
		result, err := r.Preparex(inputQuery)

		resultNamedStmt := result.(*stmt)
		assert.NoError(t, err)
		assert.Equal(t, []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2}, resultNamedStmt.primaries)
		assert.Equal(t, []*sqlx.DB{mockReadDB1, mockReadDB2}, resultNamedStmt.reads)
		assert.IsType(t, &sqlx.Stmt{}, resultNamedStmt.primaryStmts[mockPrimaryDB1])
		assert.IsType(t, &sqlx.Stmt{}, resultNamedStmt.primaryStmts[mockPrimaryDB2])
		assert.IsType(t, &sqlx.Stmt{}, resultNamedStmt.readStmts[mockReadDB1])
		assert.IsType(t, &sqlx.Stmt{}, resultNamedStmt.readStmts[mockReadDB2])
	})
}

func TestDBResolver_PreparexContext(t *testing.T) {
	t.Run("failed to prepare primary DB statement", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=?`
		result, err := r.PreparexContext(context.Background(), inputQuery)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("failed to prepare readable DB statement", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockError := errors.New("mock error")
		sqlMock.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockSecondaryDB := sqlx.NewDb(mockDB, "mock")
		r := &dbResolver{
			primaries: []*sqlx.DB{sqlx.NewDb(mockDB, "mock")},
			reads:     []*sqlx.DB{mockSecondaryDB},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=?`
		result, err := r.PreparexContext(context.Background(), inputQuery)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockDB3, sqlMock3, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock3.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockReadDB1 := sqlx.NewDb(mockDB3, "mock")
		mockDB4, sqlMock4, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock4.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockReadDB2 := sqlx.NewDb(mockDB4, "mock")
		r := &dbResolver{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			reads:     []*sqlx.DB{mockReadDB1, mockReadDB2},
		}

		inputQuery := `SELECT * FROM person WHERE first_name=?`
		result, err := r.PreparexContext(context.Background(), inputQuery)

		resultNamedStmt := result.(*stmt)
		assert.NoError(t, err)
		assert.Equal(t, []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2}, resultNamedStmt.primaries)
		assert.Equal(t, []*sqlx.DB{mockReadDB1, mockReadDB2}, resultNamedStmt.reads)
		assert.IsType(t, &sqlx.Stmt{}, resultNamedStmt.primaryStmts[mockPrimaryDB1])
		assert.IsType(t, &sqlx.Stmt{}, resultNamedStmt.primaryStmts[mockPrimaryDB2])
		assert.IsType(t, &sqlx.Stmt{}, resultNamedStmt.readStmts[mockReadDB1])
		assert.IsType(t, &sqlx.Stmt{}, resultNamedStmt.readStmts[mockReadDB2])
	})
}

func TestDBResolver_QueryRowx(t *testing.T) {
	t.Run("return error", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		result := r.QueryRowx(`SELECT * FROM person WHERE first_name=?`, "foo")

		err := result.Err()
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnRows(sqlmock.NewRows([]string{"first_name", "last_name"}).AddRow("foo", "bar"))
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		result := r.QueryRowx(`SELECT * FROM person WHERE first_name=?`, "foo")

		var person Person
		err := result.StructScan(&person)
		assert.NoError(t, err)
		expected := &Person{
			FirstName: "foo",
			LastName:  "bar",
		}
		assert.Equal(t, expected, &person)
	})
}

func TestDBResolver_QueryRowxContext(t *testing.T) {
	t.Run("return error", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		result := r.QueryRowxContext(context.Background(), `SELECT * FROM person WHERE first_name=?`, "foo")

		err := result.Err()
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnRows(sqlmock.NewRows([]string{"first_name", "last_name"}).AddRow("foo", "bar"))
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		result := r.QueryRowxContext(context.Background(), `SELECT * FROM person WHERE first_name=?`, "foo")

		var person Person
		err := result.StructScan(&person)
		assert.NoError(t, err)
		expected := &Person{
			FirstName: "foo",
			LastName:  "bar",
		}
		assert.Equal(t, expected, &person)
	})
}

func TestDBResolver_Queryx(t *testing.T) {
	t.Run("return error", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		result, err := r.Queryx(`SELECT * FROM person WHERE first_name=?`, "foo")

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnRows(
				sqlmock.NewRows([]string{"first_name", "last_name"}).
					AddRow("foo", "bar").
					AddRow("foo", "baz"),
			)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		result, err := r.Queryx(`SELECT * FROM person WHERE first_name=?`, "foo")

		assert.NoError(t, err)
		expected := []*Person{
			{
				FirstName: "foo",
				LastName:  "bar",
			},
			{
				FirstName: "foo",
				LastName:  "baz",
			},
		}
		i := 0
		for result.Next() {
			var person Person
			err := result.StructScan(&person)
			assert.NoError(t, err)
			assert.Equal(t, expected[i], &person)
			i++
		}
	})
}

func TestDBResolver_QueryxContext(t *testing.T) {
	t.Run("return error", func(t *testing.T) {
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		result, err := r.QueryxContext(context.Background(), `SELECT * FROM person WHERE first_name=?`, "foo")

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnRows(
				sqlmock.NewRows([]string{"first_name", "last_name"}).
					AddRow("foo", "bar").
					AddRow("foo", "baz"),
			)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		result, err := r.QueryxContext(context.Background(), `SELECT * FROM person WHERE first_name=?`, "foo")

		assert.NoError(t, err)
		expected := []*Person{
			{
				FirstName: "foo",
				LastName:  "bar",
			},
			{
				FirstName: "foo",
				LastName:  "baz",
			},
		}
		i := 0
		for result.Next() {
			var person Person
			err := result.StructScan(&person)
			assert.NoError(t, err)
			assert.Equal(t, expected[i], &person)
			i++
		}
	})
}

func TestDBResolver_Rebind(t *testing.T) {
	t.Run("unknown driver", func(t *testing.T) {
		mockDB, _, _ := sqlmock.New()
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		r := &dbResolver{
			primaries:    []*sqlx.DB{mockPrimaryDB},
			loadBalancer: NewRandomLoadBalancer(),
		}

		result := r.Rebind("SELECT * FROM person WHERE first_name = ?")

		assert.Equal(t, "SELECT * FROM person WHERE first_name = ?", result)
	})

	t.Run("success with dollar bindvar type", func(t *testing.T) {
		mockDB, _, _ := sqlmock.New()
		mockPrimaryDB := sqlx.NewDb(mockDB, "postgres")
		r := &dbResolver{
			primaries:    []*sqlx.DB{mockPrimaryDB},
			loadBalancer: NewRandomLoadBalancer(),
		}

		result := r.Rebind("SELECT * FROM person WHERE first_name = ?")

		assert.Equal(t, "SELECT * FROM person WHERE first_name = $1", result)
	})

	t.Run("success with question binavar type", func(t *testing.T) {
		mockDB, _, _ := sqlmock.New()
		mockPrimaryDB := sqlx.NewDb(mockDB, "mysql")
		r := &dbResolver{
			primaries:    []*sqlx.DB{mockPrimaryDB},
			loadBalancer: NewRandomLoadBalancer(),
		}

		result := r.Rebind("SELECT * FROM person WHERE first_name = ?")

		assert.Equal(t, "SELECT * FROM person WHERE first_name = ?", result)
	})

	t.Run("success with named binavar type", func(t *testing.T) {
		mockDB, _, _ := sqlmock.New()
		mockPrimaryDB := sqlx.NewDb(mockDB, "ora")
		r := &dbResolver{
			primaries:    []*sqlx.DB{mockPrimaryDB},
			loadBalancer: NewRandomLoadBalancer(),
		}

		result := r.Rebind("SELECT * FROM person WHERE first_name = ?")

		assert.Equal(t, "SELECT * FROM person WHERE first_name = :arg1", result)
	})

	t.Run("success with at binavar type", func(t *testing.T) {
		mockDB, _, _ := sqlmock.New()
		mockPrimaryDB := sqlx.NewDb(mockDB, "sqlserver")
		r := &dbResolver{
			primaries:    []*sqlx.DB{mockPrimaryDB},
			loadBalancer: NewRandomLoadBalancer(),
		}

		result := r.Rebind("SELECT * FROM person WHERE first_name = ?")

		assert.Equal(t, "SELECT * FROM person WHERE first_name = @p1", result)
	})
}

func TestDBResolver_Select(t *testing.T) {
	t.Run("return error", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		var result Person
		err := r.Select(&result, `SELECT * FROM person WHERE first_name=?`, "foo")

		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnRows(
				sqlmock.NewRows([]string{"first_name", "last_name"}).
					AddRow("foo", "bar").
					AddRow("foo", "baz"),
			)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		var result []Person
		err := r.Select(&result, `SELECT * FROM person WHERE first_name=?`, "foo")

		assert.NoError(t, err)
		expected := []Person{
			{
				FirstName: "foo",
				LastName:  "bar",
			},
			{
				FirstName: "foo",
				LastName:  "baz",
			},
		}
		assert.Equal(t, expected, result)
	})
}

func TestDBResolver_SelectContext(t *testing.T) {
	t.Run("return error", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnError(mockError)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		var result []Person
		err := r.SelectContext(context.Background(), &result, `SELECT * FROM person WHERE first_name=?`, "foo")

		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB, sqlMock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock.ExpectQuery(`SELECT * FROM person WHERE first_name=?`).
			WillReturnRows(
				sqlmock.NewRows([]string{"first_name", "last_name"}).
					AddRow("foo", "bar").
					AddRow("foo", "baz"),
			)
		mockPrimaryDB := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockSecondaryDB := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB},
			secondaries: []*sqlx.DB{mockSecondaryDB},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB, mockSecondaryDB},
		}

		var result []Person
		err := r.SelectContext(context.Background(), &result, `SELECT * FROM person WHERE first_name=?`, "foo")

		assert.NoError(t, err)
		expected := []Person{
			{
				FirstName: "foo",
				LastName:  "bar",
			},
			{
				FirstName: "foo",
				LastName:  "baz",
			},
		}
		assert.Equal(t, expected, result)
	})
}

func TestDbResolver_Unsafe(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mockDB, _, _ := sqlmock.New()
		mockPrimaryDB1 := sqlx.NewDb(mockDB, "mock")
		fakeDB, _, _ := sqlmock.New()
		mockPrimaryDB2 := sqlx.NewDb(fakeDB, "fake")
		r := &dbResolver{
			primaries:   []*sqlx.DB{mockPrimaryDB1},
			secondaries: []*sqlx.DB{mockPrimaryDB2},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
			reads: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
		}

		r.Unsafe()

		expected := sqlx.NewDb(mockDB, "mock")
		expected = expected.Unsafe()
		assert.Equal(t, expected, r.Unsafe())
	})
}