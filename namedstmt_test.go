package dbresolver

import (
	"context"
	"database/sql/driver"
	"errors"
	"math/rand"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestNamedStmt_Close(t *testing.T) {
	// TODO(proost): add failing test case.
	t.Run("success when already closed statement", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			WillBeClosed()
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockPrimaryDB1Stmt, err := mockPrimaryDB1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			WillBeClosed()
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryDB2Stmt, err := mockPrimaryDB2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			reads:     []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockPrimaryDB1: mockPrimaryDB1Stmt,
				mockPrimaryDB2: mockPrimaryDB2Stmt,
			},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockPrimaryDB1: mockPrimaryDB1Stmt,
				mockPrimaryDB2: mockPrimaryDB2Stmt,
			},
		}

		err = stmt.Close()

		assert.NoError(t, err)
	})

	t.Run("success", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			WillBeClosed()
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockPrimaryDB1Stmt, err := mockPrimaryDB1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			WillBeClosed()
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryDB2Stmt, err := mockPrimaryDB2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB3, sqlMock3, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock3.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockReadDB1 := sqlx.NewDb(mockDB3, "mock")
		mockReadDB1Stmt, err := mockReadDB1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB4, sqlMock4, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock4.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockReadDB2 := sqlx.NewDb(mockDB4, "mock")
		mockReadDB2Stmt, err := mockReadDB2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			reads:     []*sqlx.DB{mockReadDB1, mockReadDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockPrimaryDB1: mockPrimaryDB1Stmt,
				mockPrimaryDB2: mockPrimaryDB2Stmt,
			},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockReadDB1: mockReadDB1Stmt,
				mockReadDB2: mockReadDB2Stmt,
			},
		}

		err = stmt.Close()

		assert.NoError(t, err)
	})
}

func TestNamedStmt_Exec(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			ExpectExec()
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &namedStmt{
			primaries:    []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
			"last_name":  "bar",
		}
		result, err := stmt.Exec(inputArg)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, errSelectedNamedStmtNotFound)
	})

	t.Run("failed to execute query", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			ExpectExec().
			WithArgs(driver.Value("foo"), driver.Value("bar")).
			WillReturnError(mockError)
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockPrimaryDB1Stmt, err := mockPrimaryDB1.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryDB2Stmt, err := mockPrimaryDB2.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockPrimaryDB1: mockPrimaryDB1Stmt,
				mockPrimaryDB2: mockPrimaryDB2Stmt,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
			"last_name":  "bar",
		}
		result, err := stmt.Exec(inputArg)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			ExpectExec().
			WithArgs(driver.Value("foo"), driver.Value("bar")).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockPrimaryDB1Stmt, err := mockPrimaryDB1.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryDB2Stmt, err := mockPrimaryDB2.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockPrimaryDB1: mockPrimaryDB1Stmt,
				mockPrimaryDB2: mockPrimaryDB2Stmt,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
			"last_name":  "bar",
		}
		result, err := stmt.Exec(inputArg)

		assert.NoError(t, err)
		lastInsertIDResult, err := result.LastInsertId()
		assert.NoError(t, err)
		lastRowsAffected, err := result.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), lastInsertIDResult)
		assert.Equal(t, int64(1), lastRowsAffected)
	})
}

func TestNamedStmt_ExecContext(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			ExpectExec()
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &namedStmt{
			primaries:    []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
			"last_name":  "bar",
		}
		result, err := stmt.ExecContext(context.Background(), inputArg)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, errSelectedNamedStmtNotFound)
	})

	t.Run("failed to execute query", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			ExpectExec().
			WithArgs(driver.Value("foo"), driver.Value("bar")).
			WillReturnError(mockError)
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockPrimaryDB1Stmt, err := mockPrimaryDB1.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryDB2Stmt, err := mockPrimaryDB2.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockPrimaryDB1: mockPrimaryDB1Stmt,
				mockPrimaryDB2: mockPrimaryDB2Stmt,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
			"last_name":  "bar",
		}
		result, err := stmt.ExecContext(context.Background(), inputArg)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, mockError)
	})

	t.Run("success", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			ExpectExec().
			WithArgs(driver.Value("foo"), driver.Value("bar")).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockPrimaryDB1Stmt, err := mockPrimaryDB1.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryDB2Stmt, err := mockPrimaryDB2.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockPrimaryDB1: mockPrimaryDB1Stmt,
				mockPrimaryDB2: mockPrimaryDB2Stmt,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
			"last_name":  "bar",
		}
		result, err := stmt.ExecContext(context.Background(), inputArg)

		assert.NoError(t, err)
		lastInsertIDResult, err := result.LastInsertId()
		assert.NoError(t, err)
		lastRowsAffected, err := result.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), lastInsertIDResult)
		assert.Equal(t, int64(1), lastRowsAffected)
	})
}

func TestNamedStmt_Get(t *testing.T) {
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
		stmt := &namedStmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := &Person{}
		err := stmt.Get(result, inputArg)

		assert.ErrorIs(t, err, errSelectedNamedStmtNotFound)
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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := &Person{}
		err = stmt.Get(result, inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := &Person{}
		err = stmt.Get(result, inputArg)

		assert.NoError(t, err)
		expected := &Person{
			FirstName: "foo",
			LastName:  "bar",
		}
		assert.Equal(t, expected, result)
	})
}

func TestNamedStmt_GetContext(t *testing.T) {
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
		stmt := &namedStmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := &Person{}
		err := stmt.GetContext(context.Background(), result, inputArg)

		assert.ErrorIs(t, err, errSelectedNamedStmtNotFound)
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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := &Person{}
		err = stmt.GetContext(context.Background(), result, inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := &Person{}
		err = stmt.GetContext(context.Background(), result, inputArg)

		assert.NoError(t, err)
		expected := &Person{
			FirstName: "foo",
			LastName:  "bar",
		}
		assert.Equal(t, expected, result)
	})
}

func TestNamedStmt_MustExec(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			ExpectExec()
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &namedStmt{
			primaries:    []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
			"last_name":  "bar",
		}
		assert.Panics(t, func() {
			result := stmt.MustExec(inputArg)
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
		mockPrimaryDB1Stmt, err := mockPrimaryDB1.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryDB2Stmt, err := mockPrimaryDB2.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockPrimaryDB1: mockPrimaryDB1Stmt,
				mockPrimaryDB2: mockPrimaryDB2Stmt,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
			"last_name":  "bar",
		}
		assert.Panics(t, func() {
			result := stmt.MustExec(inputArg)

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
		mockPrimaryDB1Stmt, err := mockPrimaryDB1.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryDB2Stmt, err := mockPrimaryDB2.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockPrimaryDB1: mockPrimaryDB1Stmt,
				mockPrimaryDB2: mockPrimaryDB2Stmt,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
			"last_name":  "bar",
		}
		result := stmt.MustExec(inputArg)

		lastInsertIDResult, err := result.LastInsertId()
		assert.NoError(t, err)
		lastRowsAffected, err := result.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), lastInsertIDResult)
		assert.Equal(t, int64(1), lastRowsAffected)
	})
}

func TestNamedStmt_MustExecContext(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`).
			ExpectExec()
		mockPrimaryDB1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &namedStmt{
			primaries:    []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
			"last_name":  "bar",
		}
		assert.Panics(t, func() {
			result := stmt.MustExecContext(context.Background(), inputArg)
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
		mockPrimaryDB1Stmt, err := mockPrimaryDB1.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryDB2Stmt, err := mockPrimaryDB2.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockPrimaryDB1: mockPrimaryDB1Stmt,
				mockPrimaryDB2: mockPrimaryDB2Stmt,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
			"last_name":  "bar",
		}
		assert.Panics(t, func() {
			result := stmt.MustExecContext(context.Background(), inputArg)

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
		mockPrimaryDB1Stmt, err := mockPrimaryDB1.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`INSERT INTO person (first_name, last_name) VALUES (?, ?)`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryDB2Stmt, err := mockPrimaryDB2.PrepareNamed(`INSERT INTO person (first_name, last_name) VALUES (:first_name, :last_name)`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockPrimaryDB1: mockPrimaryDB1Stmt,
				mockPrimaryDB2: mockPrimaryDB2Stmt,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
			"last_name":  "bar",
		}
		result := stmt.MustExecContext(context.Background(), inputArg)

		lastInsertIDResult, err := result.LastInsertId()
		assert.NoError(t, err)
		lastRowsAffected, err := result.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), lastInsertIDResult)
		assert.Equal(t, int64(1), lastRowsAffected)
	})
}

func TestNamedStmt_Query(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &namedStmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result, err := stmt.Query(inputArg)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, errSelectedNamedStmtNotFound)
	})

	t.Run("failed to execute query", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnError(mockError)
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result, err := stmt.Query(inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result, err := stmt.Query(inputArg)

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
			err := result.Scan(&person.FirstName, &person.LastName)
			assert.NoError(t, err)
			assert.Equal(t, expected[i], &person)
			i++
		}
	})
}

func TestNamedStmt_QueryContext(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &namedStmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result, err := stmt.QueryContext(context.Background(), inputArg)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, errSelectedNamedStmtNotFound)
	})

	t.Run("failed to execute query", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnError(mockError)
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result, err := stmt.QueryContext(context.Background(), inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result, err := stmt.QueryContext(context.Background(), inputArg)

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
			err := result.Scan(&person.FirstName, &person.LastName)
			assert.NoError(t, err)
			assert.Equal(t, expected[i], &person)
			i++
		}
	})
}

func TestNamedStmt_QueryRow(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &namedStmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := stmt.QueryRow(inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := stmt.QueryRow(inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := stmt.QueryRow(inputArg)

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

func TestNamedStmt_QueryRowContext(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &namedStmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := stmt.QueryRowContext(context.Background(), inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := stmt.QueryRowContext(context.Background(), inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := stmt.QueryRowContext(context.Background(), inputArg)

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

func TestNamedStmt_QueryRowx(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &namedStmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := stmt.QueryRowx(inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := stmt.QueryRowx(inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := stmt.QueryRowx(inputArg)

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

func TestNamedStmt_QueryRowxContext(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &namedStmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := stmt.QueryRowxContext(context.Background(), inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := stmt.QueryRowxContext(context.Background(), inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result := stmt.QueryRowxContext(context.Background(), inputArg)

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

func TestNamedStmt_Queryx(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &namedStmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result, err := stmt.Queryx(inputArg)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, errSelectedNamedStmtNotFound)
	})

	t.Run("failed to execute query", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnError(mockError)
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result, err := stmt.Queryx(inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result, err := stmt.Queryx(inputArg)

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
			err := result.Scan(&person.FirstName, &person.LastName)
			assert.NoError(t, err)
			assert.Equal(t, expected[i], &person)
			i++
		}
	})
}

func TestNamedStmt_QueryxContext(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery()
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &namedStmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result, err := stmt.QueryxContext(context.Background(), inputArg)

		assert.Nil(t, result)
		assert.ErrorIs(t, err, errSelectedNamedStmtNotFound)
	})

	t.Run("failed to execute query", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mockError := errors.New("mock error")
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`).
			ExpectQuery().
			WithArgs(driver.Value("foo")).
			WillReturnError(mockError)
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result, err := stmt.QueryxContext(context.Background(), inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		result, err := stmt.QueryxContext(context.Background(), inputArg)

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
			err := result.Scan(&person.FirstName, &person.LastName)
			assert.NoError(t, err)
			assert.Equal(t, expected[i], &person)
			i++
		}
	})
}

func TestNamedStmt_Select(t *testing.T) {
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
		stmt := &namedStmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		var result []Person
		err := stmt.Select(&result, inputArg)

		assert.ErrorIs(t, err, errSelectedNamedStmtNotFound)
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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		var result []Person
		err = stmt.Select(&result, inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		var result []Person
		err = stmt.Select(&result, inputArg)

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

func TestNamedStmt_SelectContext(t *testing.T) {
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
		stmt := &namedStmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		var result []Person
		err := stmt.SelectContext(context.Background(), &result, inputArg)

		assert.ErrorIs(t, err, errSelectedNamedStmtNotFound)
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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		var result []Person
		err = stmt.SelectContext(context.Background(), &result, inputArg)

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
		mockReadStmt1, err := mockRead1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		mockReadStmt2, err := mockRead2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			reads: []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockRead1: mockReadStmt1,
				mockRead2: mockReadStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		inputArg := map[string]interface{}{
			"first_name": "foo",
		}
		var result []Person
		err = stmt.SelectContext(context.Background(), &result, inputArg)

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

func TestNamedStmt_Unsafe(t *testing.T) {
	t.Run("statement not found", func(t *testing.T) {
		mockDB1, sqlMock1, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock1.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead1 := sqlx.NewDb(mockDB1, "mock1")
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockRead2 := sqlx.NewDb(mockDB2, "mock2")
		stmt := &namedStmt{
			reads:     []*sqlx.DB{mockRead1, mockRead2},
			readStmts: map[*sqlx.DB]*sqlx.NamedStmt{},
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
		mockPrimaryStmt1, err := mockPrimaryDB1.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		mockDB2, sqlMock2, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		sqlMock2.ExpectPrepare(`SELECT * FROM person WHERE first_name=?`)
		mockPrimaryDB2 := sqlx.NewDb(mockDB2, "mock2")
		mockPrimaryStmt2, err := mockPrimaryDB2.PrepareNamed(`SELECT * FROM person WHERE first_name=:first_name`)
		assert.NoError(t, err)
		stmt := &namedStmt{
			primaries: []*sqlx.DB{mockPrimaryDB1, mockPrimaryDB2},
			primaryStmts: map[*sqlx.DB]*sqlx.NamedStmt{
				mockPrimaryDB1: mockPrimaryStmt1,
				mockPrimaryDB2: mockPrimaryStmt2,
			},
			loadBalancer: &RandomLoadBalancer{
				random: rand.New(rand.NewSource(0)),
			},
		}

		result := stmt.Unsafe()

		expected := &sqlx.NamedStmt{
			Params:      mockPrimaryStmt1.Params,
			Stmt:        mockPrimaryStmt1.Stmt.Unsafe(),
			QueryString: mockPrimaryStmt1.QueryString,
		}
		assert.Equal(t, expected, result)
	})
}
