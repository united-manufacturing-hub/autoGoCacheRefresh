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
		sleep    = flag.Int("sleep", 1, "Sleep time in minutes")
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

	for {

		var rows *sql.Rows
		rows, err = db.Query(`SELECT path, version FROM public.indexes`)
		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
			var idx Index
			err = rows.Scan(&idx.Path, &idx.Version)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("Processing %s\n", idx.Path)

			var resp *http.Response
			resp, err = http.Get(fmt.Sprintf("http://localhost:3000/%s/@latest", idx.Path))
			if err != nil {
				log.Println(err)
				continue
			}

			var body []byte
			body, err = io.ReadAll(resp.Body)
			if err != nil {
				log.Println(err)
				continue
			}

			var r Response
			err = json.Unmarshal(body, &r)
			if err != nil {
				log.Println(err)
				continue
			}

			resp.Body.Close()

			if r.Version != idx.Version {
				_, err = http.Get(fmt.Sprintf("http://localhost:3000/%s/@v/%s.info", idx.Path, idx.Version))
				if err != nil {
					log.Println(err)
					continue
				}

				_, err = http.Get(fmt.Sprintf("http://localhost:3000/%s/@v/%s.mod", idx.Path, idx.Version))
				if err != nil {
					log.Println(err)
					continue
				}

				_, err = http.Get(fmt.Sprintf("http://localhost:3000/%s/@v/%s.zip", idx.Path, idx.Version))
				if err != nil {
					log.Println(err)
					continue
				}
			}
			log.Printf("Processed %s\n", idx.Path)
		}

		err = rows.Close()
		if err != nil {
			log.Println(err)
		}

		log.Printf("Sleeping for %d minutes\n", *sleep)
		time.Sleep(time.Minute * time.Duration(*sleep))
	}

}
