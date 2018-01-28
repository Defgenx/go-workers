package main

import (
	"fmt"
	"github.com/go-workers/drivers/mysql"
)

func main() {
	fmt.Println("Starting test worker mysql...")
	_, msg, _ := mysql.Execute("SELECT ID, DESCRIPTION, LOGIN FROM TEST LIMIT 1")

	for {
		m, _ := <-msg
		for m.Next() {
			var id int
			var desc, login string
			m.Scan(&id, &desc, &login)
			fmt.Printf("Row : %d / %s / %s\n\n", id, desc, login)
		}
		if !m.NextResultSet() {
			fmt.Println("No more results, next query...")
			break
		}

	}
		db, msgs, _ := mysql.Execute("SELECT ID, DESCRIPTION, LOGIN FROM TEST WHERE ID = ?", 372)

	for {
		ms, _ := <-msgs
		for ms.Next() {
			var id int
			var desc, login string
			ms.Scan(&id, &desc, &login)
			fmt.Printf("Row : %d / %s / %s\n\n", id, desc, login)
		}
		if !ms.NextResultSet() {
			fmt.Println("No more results, next query...")
			break;
		}
	}
	db.Shutdown()
	fmt.Println("No more message, shutting worker mysql...")
	return
}
