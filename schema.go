package main

import (
	"encoding/json"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// TODO small workaround to serialize the schema into immudb

type Schema []*Column

type Column struct {
	Name       string
	Type       string
	Source     string
	PrimaryKey bool
}

func NewSchema(s sql.Schema) Schema {
	var out Schema
	for _, c := range s {
		out = append(out, &Column{
			PrimaryKey: c.PrimaryKey,
			Source:     c.Source,
			Name:       c.Name,
			Type:       c.Type.Type().String(),
		})
	}

	return out
}

func SchemaFromData(data []byte) (sql.Schema, error) {
	var ss Schema
	if err := json.Unmarshal(data, &ss); err != nil {
		return nil, err
	}

	var out sql.Schema
	for _, c := range ss {
		var sqlt sql.Type
		switch c.Type {
		case "TEXT":
			sqlt = sql.CreateLongText(sql.Collation_Default)
			break
		case "INT32":
			t, err := sql.CreateNumberType(query.Type_INT64)
			if err != nil {
				return nil, err
			}
			sqlt = t
			break
		case "TIMESTAMP":
			t, err := sql.CreateDatetimeType(query.Type_TIMESTAMP)
			if err != nil {
				return nil, err
			}
			sqlt = t
			break
		default:
			return nil, fmt.Errorf("unsupported type: %s", c.Type)
		}

		col := &sql.Column{Name: c.Name, Type: sqlt, Source: c.Source, PrimaryKey: c.PrimaryKey}
		out = append(out, col)
	}

	return out, nil
}

func GetPKIndex(schema sql.Schema) (int, error) {
	for i, c := range schema {
		if c.PrimaryKey {
			return i, nil
		}
	}

	return 0, fmt.Errorf("primary key not found")
}
