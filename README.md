# dbresolver

dbresolver is [sqlx](https://github.com/jmoiron/sqlx) resolver and wrapper for database cluster.

[![Go](https://github.com/proost/dbresolver/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/proost/dbresolver/actions/workflows/ci.yml)

## Install

```shell
go get github.com/proost/dbresolver
```

## Usage

```go
package main

import (
	"context"
	"fmt"
	"log"
	
	"github.com/jmoiron/sqlx"
	"github.com/proost/dbresolver"
)

func main() {
    var (
        primaryHost       = "localhost"
        primaryPort       = 3306
        primaryUser       = "primary"
        primaryPassword   = "<password>"
        secondaryHost     = "localhost"
        secondaryPort     = 3307
        secondaryUser     = "secondary"
        secondaryPassword = "<password>"
        dbname            = "<dbname>"
    )
    // DSNs
    primaryDSN := fmt.Sprintf(
        "%s:%s@tcp(%s:%d)/%s",
        primaryUser,
        primaryPassword,
        primaryHost,
        primaryPort,
        dbname,
    )
    secondaryDSN := fmt.Sprintf(
        "%s:%s@tcp(%s:%d)/%s",
        secondaryUser,
        secondaryPassword,
        secondaryHost,
        secondaryPort,
        dbname,
    )

    // connect to primary
    primaryDB := sqlx.MustOpen("mysql", primaryDSN)
    // connect to secondary
    secondaryDB := sqlx.MustOpen("mysql", secondaryDSN)

    primaryDBsCfg := &dbresolver.PrimaryDBsConfig{
      DBs:             []*sqlx.DB{primaryDB},
      ReadWritePolicy: dbresolver.ReadWrite,
    }
    resolver := dbresolver.MustNewDBResolver(primaryDBsCfg, dbresolver.WithSecondaryDBs(secondaryDB))
    defer resolver.Close()

    resolver.MustExecContext(context.Background(), "INSERT INTO users (name) VALUES (?)", "foo")
    result, err := resolver.QueryxContext(context.Background(), `SELECT * FROM users WHERE name = "foo"`)
    if err != nil {
      log.Panic(err)
    }

    fmt.Println(result)
}
```

## Important Notes

- Primary Database will be used when you call these functions
    - `Begin`
    - `BeginTx`
    - `BeginTxx`
    - `Beginx`
    - `Conn`
    - `Connx`
    - `Exec`
    - `ExecContext`
    - `MustBegin`
    - `MustBeginTx`
    - `MustExec`
    - `MustExecContext`
    - `NamedExec`
    - `NamedExecContext`
- Readable Database(Secondary Database or Primary Database depending on configuration) will be used when you call these functions
    - `Get`
    - `GetContext`
    - `NamedQuery`
    - `NamedQueryContext`
    - `Query`
    - `QueryContext`
    - `QueryRow`
    - `QueryRowContext`
    - `QueryRowx`
    - `QueryRowxContext`
    - `Select`
    - `SelectContext`

## Contribution

To contribute to this project, you can open a PR or an issue.
