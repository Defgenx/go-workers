package mysql

import (
	"fmt"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

var (
	host     = "localhost"
	login    = "guest"
	password = "guest"
	database = "test-db"
	port     = 3306
	protocol = "tcp"
	db       *Mysql
)

func Execute(query string, parameters ... interface{}) (*Mysql, <-chan *sql.Rows, error) {
	var err error
	db, err = db.Refresh()
	var result <-chan *sql.Rows
	if len(parameters) == 0 {
		result = db.Handle(query)
	} else {
		result = db.HandleWithParameters(query, parameters...)
	}
	return db, result, err
}

type Mysql struct {
	conn *sql.DB
	done chan error
}

func NewMysql(host string, login string, password string, database string, port int, protocol string) (*Mysql, error) {
	var err error
	db = &Mysql{
		conn: nil,
	}
	netAddr := fmt.Sprintf("%s(%s:%d)", protocol, host, port)
	dsn := fmt.Sprintf("%s:%s@%s/%s", login, password, netAddr, database)
	db.conn, err = sql.Open("mysql", dsn)

	if err != nil {
		return nil, fmt.Errorf("dial: %s", err.Error())
	}

	return db, err
}

func (c *Mysql) Shutdown() error {
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("mySQL connection close error: %s", err)
	}

	defer log.Printf("mySQL shutdown OK")
	return nil
}

func (c *Mysql) Refresh() (*Mysql, error) {
	var err error
	if c == nil || c.conn.Ping() != nil {
		if c, err = NewMysql(host, login, password, database, port, protocol); err != nil {
			return nil, fmt.Errorf("mySQL connection refresh error: %s", err)
		}
		defer log.Printf("mySQL refresh OK")
	} else {
		log.Print("connection already opened")
	}
	return c, nil
}

func (c *Mysql) Handle(query string) <-chan *sql.Rows {
	out := make(chan *sql.Rows)
	go func() {
		rows, err := c.conn.Query(query)
		log.Println("handle: query finished")
		if err != nil {
			log.Fatalf("query Error: %s", err.Error())
		}
		out <- rows

	}()

	return out
}

func (c *Mysql) HandleWithParameters(query string, values ... interface{}) <-chan *sql.Rows {
	// Prepare statement for reading data
	stmtOut, err := c.conn.Prepare(query)
	if err != nil {
		log.Fatalf("prepare Query Error: %s", err.Error())
	}
	log.Println("handle: prepared query finished")

	out := make(chan *sql.Rows)
	go func() {
		rows, err := stmtOut.Query(values...)
		if err != nil {
			log.Fatalf("query Error: %s", err.Error())
		}
		log.Println("handle: query finished")
		out <- rows
	}()

	return out
}
