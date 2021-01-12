package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/olekukonko/tablewriter"
)

func main() {
	catalog := sql.NewCatalog()

	db, err := NewImmuDatabase()
	if err != nil {
		log.Fatal("error creating database: ", err)
	}

	catalog.AddDatabase(db)

	e := sqle.New(catalog, analyzer.NewDefault(catalog), &sqle.Config{})

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("    ImmuSQL demo     ")
	fmt.Println("---------------------")

	for {
		fmt.Print("-> ")
		text, _ := reader.ReadString('\n')
		// convert CRLF to LF
		text = strings.Replace(text, "\n", "", -1)

		schema, rows, err := e.Query(sql.NewContext(context.Background()).WithCurrentDB("immudb"), text)

		if err != nil {
			log.Println("query error: ", err)
			continue
		}

		table := tablewriter.NewWriter(os.Stdout)
		var header []string
		for _, c := range schema {
			header = append(header, c.Name)
		}
		table.SetHeader(header)

		for {
			r, err := rows.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal("error iterating rows: ", err)
			}

			var rowString []string
			for _, f := range r {
				rowString = append(rowString, fmt.Sprintf("%v", f))
			}
			table.Append(rowString)
		}

		table.Render()

		if err := rows.Close(); err != nil {
			log.Fatal("error closing row iterator", err)
		}
	}
}
