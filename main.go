package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"io"
	"log"
	"net/http"
	"time"
)

type Index struct {
	Path    string
	Version string
}

type Response struct {
	Version string `json:"Version"`
}

func main() {
	var (
		host     = flag.String("host", "localhost", "DB host")
		port     = flag.Int("port", 5432, "DB port")
		user     = flag.String("user", "", "DB user")
		password = flag.String("password", "", "DB password")
		dbname   = flag.String("dbname", "", "DB name")
	)
	flag.Parse()

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		*host, *port, *user, *password, *dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query(`SELECT path, version FROM public.indexes`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var idx Index
		err = rows.Scan(&idx.Path, &idx.Version)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Processing %s\n", idx.Path)

		resp, err := http.Get(fmt.Sprintf("http://localhost:3000/%s/@latest", idx.Path))
		if err != nil {
			log.Fatal(err)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}

		var r Response
		err = json.Unmarshal(body, &r)
		if err != nil {
			log.Fatal(err)
		}

		resp.Body.Close()

		if r.Version != idx.Version {
			_, err = http.Get(fmt.Sprintf("http://localhost:3000/%s/@v/%s.info", idx.Path, idx.Version))
			if err != nil {
				log.Fatal(err)
			}

			_, err = http.Get(fmt.Sprintf("http://localhost:3000/%s/@v/%s.mod", idx.Path, idx.Version))
			if err != nil {
				log.Fatal(err)
			}

			_, err = http.Get(fmt.Sprintf("http://localhost:3000/%s/@v/%s.zip", idx.Path, idx.Version))
			if err != nil {
				log.Fatal(err)
			}
		}
		time.Sleep(1 * time.Minute)
	}
}
