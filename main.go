package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/go-goracle/goracle"
)

var (
	result    [][]string
	container []string
	pointers  []interface{}

	dbuser     = flag.String("dbuser", "", "(Mandatory) - The DB username for schema")
	dbpass     = flag.String("dbpass", "", "The DB password for schema")
	url        = flag.String("url", "", "(Mandatory) The DB URL for schema <<localhost:1521/XE>>")
	stmt       = flag.String("sql", "", "(One of file or sql mandatory) The statement to execute")
	query      = flag.Bool("query", false, "Is a query or Insert/Update")
	file       = flag.String("file", "", "(One of file or sql mandatory) A file with SQL Commands")
	resultFile = flag.String("result", "result.json", "The output of query will be written to this file")
)

func main() {

	flag.Parse()

	validate()

	p := goracle.ConnectionParams{
		Username:    *dbuser,
		Password:    *dbpass,
		SID:         *url,
		MaxSessions: 5,
		MinSessions: 1,
	}
	// _ = params
	// _ = "gvth_dev_gmo_txn:gvth_dev_gmo_txn:@tcp(localhost:1521)/XE"
	db, err := sql.Open("goracle", p.String())

	if err != nil {
		panic(err)
	}

	if *query {
		fetchQuery(db)
	} else {
		updateStatement(db)
	}

	////////////////////////////////////////////

}

func fetchQuery(db *sql.DB) {
	rows, eErr := db.Query(*stmt)

	if eErr != nil {
		panic(eErr)
	}

	////////////////////////////////////////////

	cols, err := rows.Columns()
	if err != nil {
		panic(err.Error())
	}

	length := len(cols)

	result = append(result, cols)

	for rows.Next() {
		pointers = make([]interface{}, length)
		container = make([]string, length)

		for i := range pointers {
			pointers[i] = &container[i]
		}

		err = rows.Scan(pointers...)
		if err != nil {
			panic(err.Error())
		}

		result = append(result, container)
	}

	//fmt.Println(result)
	json.NewEncoder(os.Stdout).Encode(result)
}

func updateStatement(db *sql.DB) {

	db.Exec(*stmt)
}

//validate - This will check for the below conditions to be true
// dbuser -- Must be provided
// dbpassword - Optional. If not provided will default to dbuser
// url - Mandatory -  Should be of the format  ip:port/sid
// stmt - Optional between this and file
// file - Optional between stmt and file
func validate() {

	if isEmpty(*dbuser) {
		fmt.Println("dbuser not provided")
		fmt.Println("more more information type --help")
		os.Exit(-1)
	}

	if isEmpty(*dbpass) {
		dbpass = dbuser
	}

	if isEmpty(*url) {
		fmt.Println("url not provided")
		fmt.Println("more more information type --help")
		os.Exit(-1)
	}

	if isEmpty(*stmt) && isEmpty(*file) {
		fmt.Println("Either sql or file must be provided")
		fmt.Println("more more information type --help")
		os.Exit(-1)
	}

}

//Checks if string is empty or not
func isEmpty(text string) bool {
	for _, char := range text {
		if char != ' ' {
			return false
		}
	}
	return true
}
