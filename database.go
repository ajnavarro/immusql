package main

import (
	"github.com/codenotary/immudb/pkg/client"
	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.Database = &ImmuDatabase{}

// TODO it is possible to get a specific version of the data stored in the database too!
// var _ sql.VersionedDatabase = &ImmuDatabase{}
var _ sql.TableCreator = &ImmuDatabase{}

type ImmuDatabase struct {
	s *Storage
}

func NewImmuDatabase() (*ImmuDatabase, error) {
	c, err := client.NewImmuClient(client.DefaultOptions().WithAuth(false))
	if err != nil {
		return nil, err
	}

	return &ImmuDatabase{
		s: &Storage{c},
	}, nil
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
	schema, err := id.s.GetSchema(ctx, id.Name(), tblName)
	if err != nil {
		return nil, false, err
	}

	pkIndex, err := GetPKIndex(schema)
	if err != nil {
		return nil, false, err
	}

	return &ImmuTable{
		s:       id.s,
		name:    tblName,
		dbName:  id.Name(),
		schema:  schema,
		pkIndex: pkIndex,
	}, true, nil
}

// GetTableNames returns the table names of every table in the database
func (id *ImmuDatabase) GetTableNames(ctx *sql.Context) ([]string, error) {
	return id.s.GetTables(ctx, id.Name())
}

// CreateTable creates the table with the given name and schema. If a table with that name already exists, must return
// sql.ErrTableAlreadyExists.
func (id *ImmuDatabase) CreateTable(ctx *sql.Context, name string, schema sql.Schema) error {
	return id.s.CreateTable(ctx, name, id.Name(), schema)
}
