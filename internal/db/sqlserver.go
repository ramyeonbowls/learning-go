package db

import (
	"database/sql"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

func NewSQLServer(dsn string) (*sql.DB, error) {
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	return db, db.Ping()
}
