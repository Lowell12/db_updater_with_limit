package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
)

var (
	dbAddress  string
	dbName     string
	dbUser     string
	dbPassword string
	tableName  string
	filter     string
)

func main() {

	scanner := bufio.NewScanner(os.Stdin)

	printWarning(scanner)
	initDB(scanner)
}

func printWarning(scanner *bufio.Scanner) {
	fmt.Println("ROWS DELETOR - copyright Radit 2023")
	fmt.Println("Use this app with your own risk")
	fmt.Println("")
	fmt.Print("Press 'Y' if you understand the risk and want to continue: ")
	scanner.Scan()

	input := scanner.Text()
	if input != "Y" {
		log.Fatal("Program exit.")
	}
}

func initDB(scanner *bufio.Scanner) {

	fmt.Print("db address: ")
	scanner.Scan()
	dbAddress = scanner.Text()
	if dbAddress == "" {
		log.Fatal("db address empty")
	}

	fmt.Print("db name: ")
	scanner.Scan()
	dbName = scanner.Text()
	if dbName == "" {
		log.Fatal("db name empty")
	}

	fmt.Print("db user: ")
	scanner.Scan()
	dbUser = scanner.Text()
	if dbUser == "" {
		log.Fatal("db user empty")
	}

	fmt.Print("db password: ")
	scanner.Scan()
	dbPassword = scanner.Text()
	if dbPassword == "" {
		log.Fatal("db password empty")
	}

	fmt.Print("table name: ")
	scanner.Scan()
	tableName = scanner.Text()
	if tableName == "" {
		log.Fatal("table name empty")
	}

	fmt.Print("filter: ")
	scanner.Scan()
	filter = scanner.Text()
	if filter == "" {
		log.Fatal("filter is empty")
	}

	// Connect to the database
	connString := fmt.Sprintf("user=%s host=%s port=5432 password=%s dbname=%s sslmode=disable",
		dbUser,
		dbAddress,
		dbPassword,
		dbName,
	)

	db, err := sql.Open("postgres", connString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Check if the connection is working
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to the database!")

	query := fmt.Sprintf("DELETE FROM %s WHERE %s;", tableName, filter)
	fmt.Println("Query that will be executed: ", query)
	fmt.Print("Are you sure you want to execute? (Y/N) ")

	scanner.Scan()
	confirmation := scanner.Text()
	if confirmation != "Y" {
		log.Fatal("program exit.")
	}

	_, err = db.Exec(query)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Execution Done!")
}
