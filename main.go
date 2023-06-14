package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"io"
	"log"
	"math/rand"
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
		host      = flag.String("host", "localhost", "DB host")
		port      = flag.Int("port", 5432, "DB port")
		user      = flag.String("user", "", "DB user")
		password  = flag.String("password", "", "DB password")
		dbname    = flag.String("dbname", "", "DB name")
		sleep     = flag.Int("sleep", 1, "Sleep time in minutes")
		workers   = flag.Int("workers", 1, "Number of workers")
		sleeprmin = flag.Int("sleeprmin", 1, "Sleep time in seconds, after request")
		sleeprmax = flag.Int("sleeprmax", 60, "Sleep time in seconds, after request")
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

	var workChan = make(chan Index, *workers)

	for i := 0; i < *workers; i++ {
		go processor(workChan, *sleeprmin, *sleeprmax)
	}

	for {

		var rows *sql.Rows
		rows, err = db.Query(`SELECT path, version FROM public.indexes ORDER BY RANDOM ()`)
		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
			var idx Index
			err = rows.Scan(&idx.Path, &idx.Version)
			if err != nil {
				log.Fatal(err)
			}
			workChan <- idx
		}

		err = rows.Close()
		if err != nil {
			log.Println(err)
		}

		log.Printf("Sleeping for %d minutes\n", *sleep)
		time.Sleep(time.Minute * time.Duration(*sleep))
	}
}

func processor(workChan chan Index, minSleep int, maxSleep int) {
	for {
		idx := <-workChan
		process(idx)

		// Sleep random seconds between minSleep and maxSleep
		sleep := minSleep + rand.Intn(maxSleep-minSleep)
		log.Printf("Sleeping for %d seconds\n", sleep)
		time.Sleep(time.Second * time.Duration(sleep))
	}
}

func process(idx Index) {
	log.Printf("Processing %s\n", idx.Path)
	var resp *http.Response
	var err error
	resp, err = http.Get(fmt.Sprintf("http://localhost:3000/%s/@latest", idx.Path))
	if err != nil {
		log.Println(err)
		return
	}

	var body []byte
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		return
	}

	var r Response
	err = json.Unmarshal(body, &r)
	if err != nil {
		log.Printf("Failed to unmarshal %s (%v)\n", body, err)
		return
	}

	resp.Body.Close()

	if r.Version == idx.Version {
		return
	}

	_, err = http.Get(fmt.Sprintf("http://localhost:3000/%s/@v/%s.info", idx.Path, idx.Version))
	if err != nil {
		log.Println(err)
		return
	}

	_, err = http.Get(fmt.Sprintf("http://localhost:3000/%s/@v/%s.mod", idx.Path, idx.Version))
	if err != nil {
		log.Println(err)
		return
	}

	_, err = http.Get(fmt.Sprintf("http://localhost:3000/%s/@v/%s.zip", idx.Path, idx.Version))
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("Processed %s\n", idx.Path)
}
