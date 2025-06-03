package db

import (
	"database/sql"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

func ConnectSQLite(path string) (*sql.DB, error) {
	return sql.Open("sqlite3", path)
}

func ConnectPostgres(connStr string) (*sql.DB, error) {
	return sql.Open("postgres", connStr)
}
