package main

import (
	"benchmark/bulk-insert/data"
	"benchmark/bulk-insert/parsing"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type DbConfig struct {
	Dialect  string
	Host     string
	Port     string
	Username string
	Password string
	Name     string
	Charset  string
}

type DB struct {
	Db *sql.DB
}

func (DB *DB) Initialize(config DbConfig) (err error) {
	dbURI := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s&parseTime=True",
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.Name,
		config.Charset)
	if DB.Db, err = sql.Open(config.Dialect, dbURI); err != nil {
		return
	}
	if err = DB.Db.Ping(); err != nil {
		return
	}
	return
}

var nilInt json.Number

func getNullInteger(number json.Number) string {
	if number == nilInt {
		return "NULL"
	} else {
		return fmt.Sprint(number)
	}
}

func main() {
	dbConfig := DbConfig{
		Dialect:  "mysql",
		Host:     "127.0.0.1",
		Port:     "3306",
		Username: "root",
		Password: "djfrnf081@",
		Name:     "benchmark",
		Charset:  "utf8",
	}
	db := DB{}
	if err := db.Initialize(dbConfig); err != nil {
		panic(err)
	}
	for i := 0; i < 20; i++ {
		startTime := time.Now()
		doBulkInsert(db.Db)
		elapsedTime := time.Since(startTime)
		fmt.Printf("bulk insert time: %v\n", elapsedTime.Seconds())
		time.Sleep(10 * time.Second)
	}
	for i := 0; i < 20; i++ {
		startTime := time.Now()
		doSingleInsert(db.Db)
		elapsedTime := time.Since(startTime)
		fmt.Printf("single insert time: %v\n", elapsedTime.Seconds())
		time.Sleep(10 * time.Second)
	}
	
}

func doBulkInsert(db *sql.DB) {
	bucketLogTemplates := []parsing.LogTemplate{}
	for _, log := range data.Logs {
		logTemplate, _ := parsing.GetLogTemplate(log)
		bucketLogTemplates = append(bucketLogTemplates, logTemplate)
		if len(bucketLogTemplates) > 50 {
			query := "INSERT INTO bulk_insert_table (time,id,type,person,number,age,country) VALUES"
			for _, logTemplate := range bucketLogTemplates {
				query += fmt.Sprintf(` ('%s', '%s', %s, '%s', '%s', %s, '%s'),`,
					logTemplate.Time,
					logTemplate.Head.Id,
					getNullInteger(logTemplate.Content.Type),
					logTemplate.Content.Person,
					logTemplate.Content.Number,
					getNullInteger(logTemplate.Content.Age),
					logTemplate.Content.Country,
				)
			}
			query = query[:len(query)-1]
			_, _ = db.Exec(query)
			bucketLogTemplates = nil
		}
	}
	query := "INSERT INTO bulk_insert_table (time,id,type,person,number,age,country) VALUES"
	for _, logTemplate := range bucketLogTemplates {
		query += fmt.Sprintf(` ('%s', '%s', %s, '%s', '%s', %s, '%s'),`,
			logTemplate.Time,
			logTemplate.Head.Id,
			getNullInteger(logTemplate.Content.Type),
			logTemplate.Content.Person,
			logTemplate.Content.Number,
			getNullInteger(logTemplate.Content.Age),
			logTemplate.Content.Country,
		)
	}
	query = query[:len(query)-1]
	_, _ = db.Exec(query)
	bucketLogTemplates = nil
}

func doSingleInsert(db *sql.DB) {
	for _, log := range data.Logs {
		logTemplate, _ := parsing.GetLogTemplate(log)
		query := "INSERT INTO bulk_insert_table (time,id,type,person,number,age,country) VALUES"

		query += fmt.Sprintf(` ('%s', '%s', %s, '%s', '%s', %s, '%s')`,
			logTemplate.Time,
			logTemplate.Head.Id,
			getNullInteger(logTemplate.Content.Type),
			logTemplate.Content.Person,
			logTemplate.Content.Number,
			getNullInteger(logTemplate.Content.Age),
			logTemplate.Content.Country,
		)
		_, _ = db.Exec(query)
	}
}
