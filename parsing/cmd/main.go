package main

import (
	"benchmark-dir/parsed-insert/data"
	"benchmark-dir/parsed-insert/notparsing"
	"benchmark-dir/parsed-insert/parsing"
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
		Username: "dongjulee",
		Password: "djfrnf081@",
		Name:     "mysql",
		Charset:  "utf8",
	}
	db := DB{}
	if err := db.Initialize(dbConfig); err != nil {
		panic(err)
	}
	for i := 0; i < 20; i++ {
		startTime := time.Now()
		doParseInsert(db.Db)
		//doNotParseInsert(db.Db)
		elapsedTime := time.Since(startTime)
		fmt.Printf("parse 실행시간: %v\n", elapsedTime.Seconds())
		time.Sleep(10 * time.Second)
	}
	for i := 0; i < 20; i++ {
		startTime := time.Now()
		//doParseInsert(db.Db)
		doNotParseInsert(db.Db)
		elapsedTime := time.Since(startTime)
		fmt.Printf("not parse 실행시간: %v\n", elapsedTime.Seconds())
		time.Sleep(10 * time.Second)
	}
}

func doParseInsert(db *sql.DB) {
	stmt, _ := db.Prepare("INSERT INTO parse (time,id,type,person,number,age,country) VALUES ( ?, ?, ?, ?, ?, ?, ?)")
	for _, log := range data.Logs {
		logTemplate, _ := parsing.GetLogTemplate(log)
		_, err := stmt.Exec(
			logTemplate.Time,
			logTemplate.Head.Id,
			logTemplate.Content.Type,
			logTemplate.Content.Person,
			logTemplate.Content.Number,
			logTemplate.Content.Age,
			logTemplate.Content.Country,
		)
		if err != nil {
			panic(err)
		}
	}
}

func doNotParseInsert(db *sql.DB) {
	stmt, _ := db.Prepare("INSERT INTO not_parse (time,content) VALUES ( ?, ?)")
	for _, log := range data.Logs {
		logTemplate, _ := notparsing.GetLogTemplate(log)
		_, err := stmt.Exec(
			logTemplate.Time,
			logTemplate.Content,
		)
		if err != nil {
			panic(err)
		}
	}
}
