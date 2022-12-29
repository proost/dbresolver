package dbresolver

import (
	"context"
	"database/sql/driver"
	"math/rand"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestStmt_Get(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &stmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := &Person{}
		err := stmt.Get(result, "foo")

		assert.ErrorIs(t, err, errSelectedStmtNotFound)
	})

	t.Run("failed to execute query", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnError(mockError)
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := &Person{}
		err = stmt.Get(result, "foo")

		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnRows(sqlmock.NewRows([]string{"first_name", "last_name"}).
				AddRow("foo", "bar"))
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := &Person{}
		err = stmt.Get(result, "foo")

		assert.NoError(t, err)
		expected := &Person{
			FirstName: "foo",
			LastName:  "bar",
		}
		assert.Equal(t, expected, result)
	})
}

func TestStmt_GetContext(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &stmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := &Person{}
		err := stmt.GetContext(context.Background(), result, "foo")

		assert.ErrorIs(t, err, errSelectedStmtNotFound)
	})

	t.Run("failed to execute query", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnError(mockError)
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := &Person{}
		err = stmt.GetContext(context.Background(), result, "foo")

		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnRows(sqlmock.NewRows([]string{"first_name", "last_name"}).
				AddRow("foo", "bar"))
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := &Person{}
		err = stmt.GetContext(context.Background(), result, "foo")

		assert.NoError(t, err)
		expected := &Person{
			FirstName: "foo",
			LastName:  "bar",
		}
		assert.Equal(t, expected, result)
	})
}

func TestStmt_MustExec(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			ExpectExec()
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &stmt{
			primaries:    []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.Stmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		assert.Panics(t, func() {
			result := stmt.MustExec("foo", "bar")
			assert.Nil(t, result)
		})
	})

	t.Run("failed to execute query", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			ExpectExec().
			WithArgs(driver.Value("foo"), driver.Value("bar")).
			WillReturnError(mockError)
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockPrimaryDB1Stmt, err := mockPrimaryDB1.Preparex(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryDB2Stmt, err := mockPrimaryDB2.Preparex(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		assert.NoError(t, err)
		stmt := &stmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockPrimaryDB1: mockPrimaryDB1Stmt,
				mockPrimaryDB2: mockPrimaryDB2Stmt,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		assert.Panics(t, func() {
			result := stmt.MustExec("foo", "bar")

			assert.Nil(t, result)
		})
	})

	t.Run("success", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			ExpectExec().
			WithArgs(driver.Value("foo"), driver.Value("bar")).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockPrimaryDB1Stmt, err := mockPrimaryDB1.Preparex(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryDB2Stmt, err := mockPrimaryDB2.Preparex(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		assert.NoError(t, err)
		stmt := &stmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockPrimaryDB1: mockPrimaryDB1Stmt,
				mockPrimaryDB2: mockPrimaryDB2Stmt,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := stmt.MustExec("foo", "bar")

		lastInsertIDResult, err := result.LastInsertId()
		assert.NoError(t, err)
		lastRowsAffected, err := result.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), lastInsertIDResult)
		assert.Equal(t, int64(1), lastRowsAffected)
	})
}

func TestStmt_MustExecContext(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			ExpectExec()
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &stmt{
			primaries:    []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.Stmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		assert.Panics(t, func() {
			result := stmt.MustExecContext(context.Background(), "foo", "bar")
			assert.Nil(t, result)
		})
	})

	t.Run("failed to execute query", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			ExpectExec().
			WithArgs(driver.Value("foo"), driver.Value("bar")).
			WillReturnError(mockError)
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockPrimaryDB1Stmt, err := mockPrimaryDB1.Preparex(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryDB2Stmt, err := mockPrimaryDB2.Preparex(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		assert.NoError(t, err)
		stmt := &stmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockPrimaryDB1: mockPrimaryDB1Stmt,
				mockPrimaryDB2: mockPrimaryDB2Stmt,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		assert.Panics(t, func() {
			result := stmt.MustExecContext(context.Background(), "foo", "bar")

			assert.Nil(t, result)
		})
	})

	t.Run("success", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			ExpectExec().
			WithArgs(driver.Value("foo"), driver.Value("bar")).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockPrimaryDB1Stmt, err := mockPrimaryDB1.Preparex(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryDB2Stmt, err := mockPrimaryDB2.Preparex(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		assert.NoError(t, err)
		stmt := &stmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockPrimaryDB1: mockPrimaryDB1Stmt,
				mockPrimaryDB2: mockPrimaryDB2Stmt,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := stmt.MustExecContext(context.Background(), "foo", "bar")

		lastInsertIDResult, err := result.LastInsertId()
		assert.NoError(t, err)
		lastRowsAffected, err := result.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), lastInsertIDResult)
		assert.Equal(t, int64(1), lastRowsAffected)
	})
}

func TestStmt_QueryRowx(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &stmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := stmt.QueryRowx("foo")

		assert.Nil(t, result)
	})

	t.Run("failed to execute query", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnError(mockError)
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := stmt.QueryRowx("foo")

		err = result.Err()
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnRows(sqlmock.NewRows([]string{"first_name", "last_name"}).
				AddRow("foo", "bar"))
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := stmt.QueryRowx("foo")

		assert.NoError(t, err)
		expected := &Person{
			FirstName: "foo",
			LastName:  "bar",
		}
		var actual Person
		err = result.StructScan(&actual)
		assert.NoError(t, err)
		assert.Equal(t, expected, &actual)
	})
}

func TestStmt_QueryRowxContext(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &stmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := stmt.QueryRowxContext(context.Background(), "foo")

		assert.Nil(t, result)
	})

	t.Run("failed to execute query", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnError(mockError)
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := stmt.QueryRowxContext(context.Background(), "foo")

		err = result.Err()
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnRows(sqlmock.NewRows([]string{"first_name", "last_name"}).
				AddRow("foo", "bar"))
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := stmt.QueryRowxContext(context.Background(), "foo")

		assert.NoError(t, err)
		expected := &Person{
			FirstName: "foo",
			LastName:  "bar",
		}
		var actual Person
		err = result.StructScan(&actual)
		assert.NoError(t, err)
		assert.Equal(t, expected, &actual)
	})
}

func TestStmt_Queryx(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &stmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result, err := stmt.Queryx("foo")

		assert.Nil(t, result)
		assert.ErrorIs(t, err, errSelectedStmtNotFound)
	})

	t.Run("failed to execute query", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnError(mockError)
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result, err := stmt.Queryx("foo")

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnRows(sqlmock.NewRows([]string{"first_name", "last_name"}).
				AddRow("foo", "bar").
				AddRow("foo", "baz"))
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result, err := stmt.Queryx("foo")

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

func TestStmt_QueryxContext(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &stmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result, err := stmt.QueryxContext(context.Background(), "foo")

		assert.Nil(t, result)
		assert.ErrorIs(t, err, errSelectedStmtNotFound)
	})

	t.Run("failed to execute query", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnError(mockError)
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result, err := stmt.QueryxContext(context.Background(), "foo")

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnRows(sqlmock.NewRows([]string{"first_name", "last_name"}).
				AddRow("foo", "bar").
				AddRow("foo", "baz"))
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result, err := stmt.QueryxContext(context.Background(), "foo")

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

func TestStmt_Select(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &stmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		var result []Person
		err := stmt.Select(&result, "foo")

		assert.ErrorIs(t, err, errSelectedStmtNotFound)
	})

	t.Run("failed to execute query", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnError(mockError)
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		var result []Person
		err = stmt.Select(&result, "foo")

		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnRows(sqlmock.NewRows([]string{"first_name", "last_name"}).
				AddRow("foo", "bar").
				AddRow("foo", "baz"))
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		var result []Person
		err = stmt.Select(&result, "foo")

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

func TestStmt_SelectContext(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &stmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := &Person{}
		err := stmt.SelectContext(context.Background(), result, "foo")

		assert.ErrorIs(t, err, errSelectedStmtNotFound)
	})

	t.Run("failed to execute query", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnError(mockError)
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := &Person{}
		err = stmt.SelectContext(context.Background(), result, "foo")

		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		type Person struct {
			FirstName string `db:"first_name"`
			LastName  string `db:"last_name"`
		}
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnRows(sqlmock.NewRows([]string{"first_name", "last_name"}).
				AddRow("foo", "bar").
				AddRow("foo", "baz"))
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		var result []Person
		err = stmt.SelectContext(context.Background(), &result, "foo")

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

func TestStmt_Unsafe(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &stmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.Stmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := stmt.Unsafe()

		assert.Nil(t, result)
	})

	t.Run("success", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockPrimaryStmt1, err := mockPrimaryDB1.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryStmt2, err := mockPrimaryDB2.Preparex(`SELECT * FROM person WHERE first_name=?`)
		assert.NoError(t, err)
		stmt := &stmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.Stmt{
				mockPrimaryDB1: mockPrimaryStmt1,
				mockPrimaryDB2: mockPrimaryStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := stmt.Unsafe()

		expected := &sqlx.Stmt{
			Stmt:   mockPrimaryStmt1.Stmt,
			Mapper: mockPrimaryStmt1.Mapper,
		}
		expected = expected.Unsafe()
		assert.Equal(t, expected, result)
	})
}
