package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type ConcreteSQLDB struct {
	db *sql.DB
}

func (db *ConcreteSQLDB) Close() error {
	return db.db.Close()
}

func (db *ConcreteSQLDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *ConcreteSQLDB) CleanupThread(ctx context.Context) {
}

func (db *ConcreteSQLDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf("SELECT * FROM %s WHERE ycsb_key = ?", table)
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s WHERE ycsb_key = ?", strings.Join(fields, ","), table)
	}

	rows, err := db.db.QueryContext(ctx, query, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil // Not found
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Scan logic for dynamic columns
	vals := make([]interface{}, len(cols))
	valPtrs := make([]interface{}, len(cols))
	for i := range vals {
		valPtrs[i] = &vals[i]
	}

	if err := rows.Scan(valPtrs...); err != nil {
		return nil, err
	}

	result := make(map[string][]byte)
	for i, col := range cols {
		if b, ok := vals[i].([]byte); ok {
			result[col] = b
		} else if s, ok := vals[i].(string); ok {
			result[col] = []byte(s)
		}
	}
	return result, nil
}

func (db *ConcreteSQLDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf("SELECT * FROM %s WHERE ycsb_key >= ? ORDER BY ycsb_key ASC LIMIT ?", table)
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s WHERE ycsb_key >= ? ORDER BY ycsb_key ASC LIMIT ?", strings.Join(fields, ","), table)
	}

	rows, err := db.db.QueryContext(ctx, query, startKey, count)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string][]byte
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}

		if err := rows.Scan(valPtrs...); err != nil {
			return nil, err
		}

		rowMap := make(map[string][]byte)
		for i, col := range cols {
			if b, ok := vals[i].([]byte); ok {
				rowMap[col] = b
			} else if s, ok := vals[i].(string); ok {
				rowMap[col] = []byte(s)
			}
		}
		results = append(results, rowMap)
	}
	return results, nil
}

func (db *ConcreteSQLDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	var sets []string
	var args []interface{}
	for k, v := range values {
		sets = append(sets, fmt.Sprintf("%s = ?", k))
		args = append(args, v)
	}
	args = append(args, key)

	query := fmt.Sprintf("UPDATE %s SET %s WHERE ycsb_key = ?", table, strings.Join(sets, ","))
	_, err := db.db.ExecContext(ctx, query, args...)
	return err
}

func (db *ConcreteSQLDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Special handling for CREATE TABLE on first insert?
	// YCSB usually assumes schema exists or creates it in load phase.
	// But go-ycsb drivers often do it lazily or explicit create.
	// We'll assume table created.

	var cols []string
	var placeholders []string
	var args []interface{}

	cols = append(cols, "ycsb_key")
	placeholders = append(placeholders, "?")
	args = append(args, key)

	for k, v := range values {
		cols = append(cols, k)
		placeholders = append(placeholders, "?")
		args = append(args, v)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ","), strings.Join(placeholders, ","))
	_, err := db.db.ExecContext(ctx, query, args...)
	return err
}

func (db *ConcreteSQLDB) Delete(ctx context.Context, table string, key string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE ycsb_key = ?", table)
	_, err := db.db.ExecContext(ctx, query, key)
	return err
}

type concreteSQLCreator struct{}

func (c concreteSQLCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	dsn := p.GetString("dsn", "file:/tmp/benchycsb.db?vfs=concretesql")
	db, err := sql.Open("concretesql", dsn)
	if err != nil {
		return nil, err
	}

	// Create table if not exists?
	// YCSB 'load' phase does not strictly guarantee create table call on DB interface?
	// Actually go-ycsb has no CreateTable method in DB interface.
	// We should probably do it here or in Insert if it fails.
	// Let's do it here. YCSB standard table is 'usertable'.
	// Fields are field0, field1 ... field9 usually (10 fields).
	// We can cheat and just create it rigid.

	// Better: Use `CREATE TABLE IF NOT EXISTS usertable (ycsb_key VARCHAR(255) PRIMARY KEY, field0 TEXT, field1 TEXT, field2 TEXT, field3 TEXT, field4 TEXT, field5 TEXT, field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT)`

	// We loop to create fields just in case
	var fields []string
	for i := 0; i < 10; i++ {
		fields = append(fields, fmt.Sprintf("field%d TEXT", i))
	}

	if p.GetBool(prop.DoTransactions, true) == false {
		_, err = db.Exec("DROP TABLE IF EXISTS usertable")
		if err != nil {
			return nil, err
		}
	}

	if p.GetBool(prop.DoTransactions, true) == false {
		_, err = db.Exec("DROP TABLE IF EXISTS usertable")
		if err != nil {
			return nil, err
		}
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS usertable (ycsb_key VARCHAR(255) PRIMARY KEY, %s)", strings.Join(fields, ", "))
	_, err = db.Exec(query)
	if err != nil {
		return nil, err
	}

	return &ConcreteSQLDB{db: db}, nil
}

func init() {
	ycsb.RegisterDBCreator("concretesql", concreteSQLCreator{})
}
