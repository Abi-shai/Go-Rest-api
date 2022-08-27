package main

import (
	"database/sql"
	"log"
	"maillinglist/jsonapi"
	"maillinglist/maildatabase"
	"sync"

	"github.com/alexflint/go-arg"
)

var args struct {
	DbPath   string `arg:"env:MAILINGLIST_DB"`
	BindJson string `arg:"env:MAILINGLIST_BIND_JSON"`
}

func main() {
	arg.MustParse(&args)

	if args.DbPath == "" {
		args.DbPath = "../list.db"
	}

	if args.BindJson == "" {
		args.BindJson = ":8080"
	}

	log.Printf("using database '%v'\n", args.DbPath)
	db, err := sql.Open("sqlite3", args.DbPath)

	if err != nil {
		log.Fatalf("Failed to open database with err: %v", err)
	}

	defer db.Close()

	maildatabase.TryCreateDatabase(db)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		log.Printf("Starting json api server...\n")
		jsonapi.Serve(db, args.BindJson)
		wg.Done()
	}()

	wg.Wait()
}
