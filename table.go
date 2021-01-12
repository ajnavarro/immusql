package main

import (
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.Table = &ImmuTable{}
var _ sql.PartitionCounter = &ImmuTable{}
var _ sql.InsertableTable = &ImmuTable{}

type ImmuTable struct {
	s       *Storage
	name    string
	dbName  string
	txID    uint64
	schema  sql.Schema
	pkIndex int
}

func (it *ImmuTable) PartitionCount(*sql.Context) (int64, error) {
	return 1, nil
}

func (it *ImmuTable) Name() string {
	return it.name
}

func (it *ImmuTable) String() string {
	return fmt.Sprintf("IMMU_TABLE[%s]", it.name)
}

func (it *ImmuTable) Inserter(*sql.Context) sql.RowInserter {
	return &ImmuRowInserter{
		s:       it.s,
		dbName:  it.dbName,
		tName:   it.name,
		pkIndex: it.pkIndex,
	}
}

func (it *ImmuTable) Schema() sql.Schema {
	return it.schema
}

func (it *ImmuTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return &dummyPartitionIter{}, nil
}

func (it *ImmuTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	rows, err := it.s.GetRows(ctx, it.txID, it.dbName, it.name)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(rows...), nil
}

var _ sql.RowInserter = &ImmuRowInserter{}

type ImmuRowInserter struct {
	s             *Storage
	dbName, tName string
	pkIndex       int

	// TODO: rows in memory before bulk insert for now
	rows []sql.Row
}

func (iri *ImmuRowInserter) Insert(ctx *sql.Context, row sql.Row) error {
	iri.rows = append(iri.rows, row)
	return nil
}

func (iri *ImmuRowInserter) Close(ctx *sql.Context) error {
	_, err := iri.s.InsertRows(ctx, iri.dbName, iri.tName, iri.pkIndex, iri.rows)
	iri.rows = nil
	return err
}

type dummyPartitionIter struct {
	called bool
}

func (dpi *dummyPartitionIter) Close() error {
	return nil
}

func (dpi *dummyPartitionIter) Next() (sql.Partition, error) {
	if dpi.called {
		return nil, io.EOF
	}

	dpi.called = true

	return &dummyPartition{}, nil
}

type dummyPartition struct {
}

func (dpi *dummyPartition) Key() []byte {
	return []byte("dummy")
}
