package database

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type Database struct {
	db *sql.DB
}

func New(connectionString string) (*Database, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return &Database{db: db}, nil
}

func (d *Database) Exec(ddl string) error {
	_, err := d.db.Exec(ddl)
	if err != nil {
		return fmt.Errorf("exec DDL: %w", err)
	}
	return nil
}

func (d *Database) Close() error {
	return d.db.Close()
}
