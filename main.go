package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

var (
	dbAddress  string
	dbName     string
	dbUser     string
	dbPassword string
)

func main() {

	scanner := bufio.NewScanner(os.Stdin)

	printWarning(scanner)
	initDB(scanner)
}

func printWarning(scanner *bufio.Scanner) {
	fmt.Println("Data Sweeper - copyright Lowell 2023")
	fmt.Println("Use this app with your own risk")
	fmt.Println("")
	fmt.Print("Press 'Y' if you understand the risk and want to continue: ")
	scanner.Scan()

	input := scanner.Text()
	if !isInputY(input) {
		log.Fatal("Program exit.")
	}
}

func convertSecondsToMinutesAndHours(seconds int) (int, int, int) {
	minutes := seconds / 60
	remainingSeconds := seconds % 60
	hours := minutes / 60
	remainingMinutes := minutes % 60

	return hours, remainingMinutes, remainingSeconds
}

func isInputY(input string) bool {
	return strings.ToUpper(input) == "Y"
}

func addInput(scanner *bufio.Scanner, descriptor string, additionalLogic ...func(...string)) string {
	fmt.Printf("%s: ", descriptor)
	scanner.Scan()
	input := scanner.Text()
	if input == "" {
		log.Fatalf("%s empty", descriptor)
	}

	for _, function := range additionalLogic {
		function(input)
	}

	return input
}

func initDB(scanner *bufio.Scanner) {

	dbAddress = addInput(scanner, "db address")

	dbName = addInput(scanner, "db name")

	dbUser = addInput(scanner, "db user")

	dbPassword = addInput(scanner, "db password")

	fmt.Println("\n=========== mini tutorial===========")
	tutorialQuery := `update [table name]
	set [set]
	where [update filter] in (
	select [select] from [table name]
	where [select filter]
	limit [limit]
	);`
	fmt.Printf("\nThe query input will be like \n%s \n", tutorialQuery)
	fmt.Println("\n=========== tutorial end ===========")
	tableName := addInput(scanner, "table name")

	set := addInput(scanner, "set")

	updateFilter := addInput(scanner, "update filter")

	selectInput := addInput(scanner, "select", func(input ...string) {
		if input[0] != updateFilter {
			log.Fatal("the [select] must have the same value as [update filter]")
		}
	})

	selectFilter := addInput(scanner, "select filter")

	limit := addInput(scanner, "limit", func(input ...string) {
		_, err := strconv.Atoi(input[0])
		if err != nil {
			log.Fatal("the [limit] must be a number")
		}
	})

	fmt.Println("\nThe repeat mentioned next input means repeat until no rows affected.")
	isRepeat := isInputY(addInput(scanner, "repeat query (Y/N)"))

	//unit in second
	sleepDurationString := ""
	if isRepeat {
		sleepDurationString = addInput(scanner, "sleep duration (seconds)", func(input ...string) {
			_, err := strconv.Atoi(input[0])
			if err != nil {
				log.Fatal("the [sleep duration] must be a number")
			}
		})
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
	fmt.Println("\nConnected to the database!")

	query := fmt.Sprintf(`update %s
	set %s
	where %s in (
	select %s from %s
	where %s
	limit %s
	);`, tableName, set, updateFilter, selectInput, tableName, selectFilter, limit)

	fmt.Println("Query that will be executed: \n", query)
	fmt.Println("repeat query = ", isRepeat)

	sleepDuration, err := strconv.Atoi(sleepDurationString)
	if err != nil {
		log.Fatal("the [sleep duration] must be a number")
	}
	hour, minutes, seconds := convertSecondsToMinutesAndHours(sleepDuration)
	fmt.Printf("Sleep time = %d hour, %d minutes, %d seconds\n", hour, minutes, seconds)

	fmt.Print("Are you sure you want to execute? (Y/N) ")

	scanner.Scan()
	confirmation := scanner.Text()
	if !isInputY(confirmation) {
		log.Fatal("program exit.")
	}

	if isRepeat {
		runningCount := 0
		limitInt, _ := strconv.Atoi(limit)
		for {
			result, err := db.Exec(query)
			if err != nil {
				log.Fatal(err)
			}

			rowsAffected, _ := result.RowsAffected()
			fmt.Println("======================")
			fmt.Printf("total affected rows = %d", rowsAffected)

			runningCount++
			fmt.Printf("\nrunning count = %d", runningCount)
			fmt.Println("\n======================")

			if rowsAffected == 0 || rowsAffected < int64(limitInt) {
				break
			}

			sleepDuration, _ := strconv.Atoi(sleepDurationString)
			time.Sleep(time.Second * time.Duration(sleepDuration))
		}
	} else {
		result, err := db.Exec(query)
		if err != nil {
			log.Fatal(err)
		}

		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("total affected rows = %d", rowsAffected)
		fmt.Println("")
	}

	fmt.Println("Execution Done!")
}
