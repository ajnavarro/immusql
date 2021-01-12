package main

import (
	"reflect"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.Database = &ImmuDatabase{}

var _ sql.VersionedDatabase = &ImmuDatabase{}
var _ sql.TableCreator = &ImmuDatabase{}

type ImmuDatabase struct {
	s *Storage
}

func NewImmuDatabase() (*ImmuDatabase, error) {
	c, err := client.NewImmuClient(client.DefaultOptions())
	if err != nil {
		return nil, err
	}

	return &ImmuDatabase{
		s: &Storage{c},
	}, nil
}

func (id *ImmuDatabase) getTable(ctx *sql.Context, tblName string, txID uint64) (sql.Table, error) {
	schema, err := id.s.GetSchema(ctx, txID, id.Name(), tblName)
	if err != nil {
		return nil, err
	}

	pkIndex, err := GetPKIndex(schema)
	if err != nil {
		return nil, err
	}

	return &ImmuTable{
		s:       id.s,
		name:    tblName,
		dbName:  id.Name(),
		schema:  schema,
		pkIndex: pkIndex,
		txID:    txID,
	}, nil
}

// GetTableInsensitiveAsOf retrieves a table by its case-insensitive name with the same semantics as
// Database.GetTableInsensitive, but at a particular revision of the database. Implementors must choose which types
// of expressions to accept as revision names.
func (id *ImmuDatabase) GetTableInsensitiveAsOf(ctx *sql.Context, tblName string, asOf interface{}) (sql.Table, bool, error) {
	switch idx := asOf.(type) {
	case int8:
		table, err := id.getTable(ctx, tblName, uint64(idx))
		if err != nil {
			return nil, false, err
		}

		return table, true, nil
	default:
		return nil, false, sql.ErrInvalidType.New(reflect.TypeOf(asOf).String())
	}
}

// GetTableNamesAsOf returns the table names of every table in the database as of the revision given. Implementors
// must choose which types of expressions to accept as revision names.
func (id *ImmuDatabase) GetTableNamesAsOf(ctx *sql.Context, asOf interface{}) ([]string, error) {
	return nil, nil
}

// Name returns the name.
func (id *ImmuDatabase) Name() string {
	return "immudb"
}

// GetTableInsensitive retrieves a table by its case insensitive name.  Implementations should look for exact
// (case-sensitive matches) first.  If no exact matches are found then any table matching the name case insensitively
// should be returned.  If there is more than one table that matches a case insensitive comparison the resolution
// strategy is not defined.
func (id *ImmuDatabase) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	table, err := id.getTable(ctx, tblName, 0)
	if err != nil {
		return nil, false, err
	}

	return table, true, nil
}

// GetTableNames returns the table names of every table in the database
func (id *ImmuDatabase) GetTableNames(ctx *sql.Context) ([]string, error) {
	return id.s.GetTables(ctx, 0, id.Name())
}

// CreateTable creates the table with the given name and schema. If a table with that name already exists, must return
// sql.ErrTableAlreadyExists.
func (id *ImmuDatabase) CreateTable(ctx *sql.Context, name string, schema sql.Schema) error {
	_, err := id.s.CreateTable(ctx, name, id.Name(), schema)
	return err
}
