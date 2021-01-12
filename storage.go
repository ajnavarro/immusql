package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
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

// dbm/db_name
const databaseMetadataKey = "dbm/%s"

type Storage struct {
	c client.ImmuClient
}

func (s *Storage) getTableMetadataKey(dbName, tableName string) []byte {
	return []byte(fmt.Sprintf(tableMetadataKey, strings.ToLower(dbName), strings.ToLower(tableName)))
}

func (s *Storage) getTableDataPrefixKey(dbName, tableName string) []byte {
	return []byte(fmt.Sprintf(tableDataPrefixKey, strings.ToLower(dbName), strings.ToLower(tableName)))
}

func (s *Storage) getTableDataKey(dbName, tableName string, primaryKey int64) []byte {
	return []byte(fmt.Sprintf(tableDataKey, strings.ToLower(dbName), strings.ToLower(tableName), primaryKey))
}

func (s *Storage) getDbMetadataKey(dbName string) []byte {
	return []byte(fmt.Sprintf(strings.ToLower(databaseMetadataKey), dbName))
}

func (s *Storage) CreateTable(ctx context.Context, tblName, dbName string, sch sql.Schema) (uint64, error) {
	// we need a primary key to make all storage logic easier
	pkFound := false
	for _, c := range sch {
		if c.PrimaryKey {
			pkFound = true
			break
		}
	}

	if !pkFound {
		return 0, fmt.Errorf("PK not found")
	}

	tables, err := s.GetTables(ctx, 0, dbName)
	if err != nil {
		return 0, err
	}

	for _, t := range tables {
		if t == strings.ToLower(tblName) {
			return 0, sql.ErrTableAlreadyExists.New(tblName)
		}
	}

	tables = append(tables, tblName)

	tablesVal, err := json.Marshal(&tables)
	if err != nil {
		return 0, err
	}

	schemaVal, err := json.Marshal(NewSchema(sch))
	if err != nil {
		return 0, err
	}

	// TODO: docu is wrong, it says that this is called KVList instead of SetRequest...
	kvList := &schema.SetRequest{KVs: []*schema.KeyValue{
		{Key: s.getDbMetadataKey(dbName), Value: tablesVal},
		{Key: s.getTableMetadataKey(dbName, tblName), Value: schemaVal},
	}}

	tx, err := s.c.SetAll(s.withLogin(ctx), kvList)
	if err != nil {
		return 0, err
	}

	return tx.Id, nil
}

func (s *Storage) InsertRows(ctx context.Context, dbName, tblName string, pkindex int, rows []sql.Row) (uint64, error) {
	var kvs []*schema.KeyValue
	for _, r := range rows {
		rr, err := json.Marshal(r)
		if err != nil {
			return 0, err
		}

		kvs = append(kvs,
			&schema.KeyValue{
				Key: s.getTableDataKey(
					dbName,
					tblName,
					r[pkindex].(int64), /*TODO: ugly type assertion to move fast*/
				),
				Value: rr,
			})
	}

	tx, err := s.c.SetAll(s.withLogin(ctx), &schema.SetRequest{KVs: kvs})
	if err != nil {
		return 0, err
	}

	return tx.Id, nil
}

func (s *Storage) GetRows(ctx context.Context, tx uint64, dbName, tblName string) ([]sql.Row, error) {
	// TODO: returning all rows at once. Immudb Scan method should return an iterator if possible.
	// We can return batches using the offset I guess, but too complicated for that demo.

	if tx == 0 {
		tx = math.MaxUint64
	}

	scanReq := &schema.ScanRequest{
		SeekKey: nil,
		Prefix:  []byte(s.getTableDataPrefixKey(dbName, tblName)),
		Desc:    false,
		Limit:   0,
		SinceTx: tx,
		NoWait:  true,
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

func (s *Storage) GetSchema(ctx context.Context, tx uint64, dbName, tblName string) (sql.Schema, error) {
	entry, err := s.c.GetSince(s.withLogin(ctx), s.getTableMetadataKey(dbName, tblName), tx)
	if err != nil {
		// TODO: SUPER UGLY. I didn't find possible error types to compare
		if strings.Contains(err.Error(), "key not found") {
			return nil, sql.ErrTableNotFound.New(tblName)
		}
		return nil, err
	}

	return SchemaFromData(entry.Value)
}

func (s *Storage) GetTables(ctx context.Context, tx uint64, dbName string) ([]string, error) {
	entry, err := s.c.GetSince(s.withLogin(ctx), s.getDbMetadataKey(dbName), tx)
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
