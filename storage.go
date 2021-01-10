package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/dolthub/go-mysql-server/sql"
	"google.golang.org/grpc/metadata"
)

// tm/database_name/table_name
const tableMetadataKey = "tm/%s/%s"

// td/database_name/table_name
const tableDataPrefixKey = "td/%s/%s"

// td/database_name/table_name/row_primary_key
const tableDataKey = tableDataPrefixKey + "/%d"

// dbm/table_name
const databaseMetadataKey = "dbm/%s"

type Storage struct {
	c client.ImmuClient
}

func (s *Storage) getTableMetadataKey(dbName, tableName string) []byte {
	return []byte(fmt.Sprintf(tableMetadataKey, strings.ToLower(dbName), strings.ToLower(tableName)))
}

func (s *Storage) getTableDataKey(dbName, tableName string, primaryKey int64) []byte {
	return []byte(fmt.Sprintf(tableDataKey, strings.ToLower(dbName), strings.ToLower(tableName), primaryKey))
}

func (s *Storage) getDbMetadataKey(dbName string) []byte {
	return []byte(fmt.Sprintf(strings.ToLower(databaseMetadataKey), dbName))
}

func (s *Storage) CreateTable(ctx context.Context, tblName, dbName string, sch sql.Schema) error {
	// we need a primary key to make all storage logic easier
	pkFound := false
	for _, c := range sch {
		if c.PrimaryKey {
			pkFound = true
			break
		}
	}

	if !pkFound {
		return fmt.Errorf("PK not found")
	}

	tables, err := s.GetTables(ctx, dbName)
	if err != nil {
		return err
	}

	for _, t := range tables {
		if t == strings.ToLower(tblName) {
			return sql.ErrTableAlreadyExists.New(tblName)
		}
	}

	tables = append(tables, tblName)

	tablesVal, err := json.Marshal(&tables)
	if err != nil {
		return err
	}

	schemaVal, err := json.Marshal(NewSchema(sch))
	if err != nil {
		return err
	}

	// TODO: docu is wrong, it says that this is called KVList instead of SetRequest...
	kvList := &schema.SetRequest{KVs: []*schema.KeyValue{
		{Key: s.getDbMetadataKey(dbName), Value: tablesVal},
		{Key: s.getTableMetadataKey(dbName, tblName), Value: schemaVal},
	}}

	_, err = s.c.SetAll(s.withLogin(ctx), kvList)
	return err
}

func (s *Storage) InsertRows(ctx context.Context, dbName, tblName string, pkindex int, rows []sql.Row) error {
	var kvs []*schema.KeyValue
	for _, r := range rows {
		rr, err := json.Marshal(r)
		if err != nil {
			return err
		}

		// TODO: ugly type assertion to move fast
		kvs = append(kvs, &schema.KeyValue{Key: s.getTableDataKey(dbName, tblName, r[pkindex].(int64)), Value: rr})
	}

	_, err := s.c.SetAll(s.withLogin(ctx), &schema.SetRequest{KVs: kvs})
	return err
}

// TODO: returning all rows at once. Immudb Scan method should return an interator if possible.
// We can return batches using the offset I guess, but too complicated for that demo.
func (s *Storage) GetRows(ctx context.Context, dbName, tblName string) ([]sql.Row, error) {
	scanReq := &schema.ScanRequest{
		SeekKey: nil,
		Prefix:  []byte(tableDataPrefixKey),
		Desc:    false,
		Limit:   0,
		SinceTx: 0,
	}

	entries, err := s.c.Scan(s.withLogin(ctx), scanReq)
	if err != nil {
		// TODO: SUPER UGLY. I didn't find possible error types to compare
		if strings.Contains(err.Error(), "no more entries") {
			return nil, nil
		}
		return nil, err
	}

	var rows []sql.Row
	for _, entry := range entries.Entries {
		var r sql.Row
		err := json.Unmarshal(entry.Value, &r)
		if err != nil {
			return nil, err
		}

		rows = append(rows, r)
	}

	return rows, nil
}

func (s *Storage) GetSchema(ctx context.Context, dbName, tblName string) (sql.Schema, error) {
	entry, err := s.c.Get(s.withLogin(ctx), s.getTableMetadataKey(dbName, tblName))
	if err != nil {
		return nil, err
	}

	return SchemaFromData(entry.Value)
}

func (s *Storage) GetTables(ctx context.Context, dbName string) ([]string, error) {
	entry, err := s.c.Get(s.withLogin(ctx), s.getDbMetadataKey(dbName))
	if err != nil {
		// TODO: SUPER UGLY. I didn't find possible error types to compare
		if strings.Contains(err.Error(), "key not found") {
			return nil, nil
		}
		return nil, err
	}

	var tables []string
	err = json.Unmarshal(entry.Value, &tables)
	return tables, err
}

// TODO: I'm logging in always because I dunno how much time will be the token valid
func (s *Storage) withLogin(ctx context.Context) context.Context {
	lr, err := s.c.Login(ctx, []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}

	md := metadata.Pairs("authorization", lr.Token)
	return metadata.NewOutgoingContext(ctx, md)
}
