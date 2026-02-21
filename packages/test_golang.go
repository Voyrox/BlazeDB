package main

import (
	"fmt"
	"os"
	"strconv"

	xeondbdriver "github.com/Voyrox/Xeondb/packages/golang"
)

func main() {
	host := os.Getenv("Xeondb_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := 9876
	if p := os.Getenv("Xeondb_PORT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil {
			port = n
		}
	}

	// Optional auth (only required if the server has auth enabled)
	username := os.Getenv("Xeondb_USERNAME")
	password := os.Getenv("Xeondb_PASSWORD")

	client := xeondbdriver.NewXeondbClient(xeondbdriver.Options{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
	})

	keyspace := "testKeyspace"
	table := "testTable"

	fmt.Printf("Connecting to Xeondb at %s:%d...\n", host, port)
	connected := client.Connect()
	if !connected {
		panic("Unable to establish a connection to the database.")
	}
	fmt.Println("Successfully connected to the database.")
	defer func() {
		client.Close()
		fmt.Println("Database connection closed.")
	}()

	_, _ = client.QueryTable(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s;", keyspace))
	if err := client.SelectKeyspace(keyspace); err != nil {
		panic(err)
	}
	fmt.Println("Using keyspace:", keyspace)

	_, _ = client.QueryTable(
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id int64, value varchar, PRIMARY KEY (id));", table),
	)
	fmt.Printf("Table '%s' is ready.\n", table)

	_, _ = client.QueryTable(
		fmt.Sprintf("INSERT INTO %s (id, value) VALUES (1, \"hello\"), (2, \"world\");", table),
	)
	fmt.Println("Inserted initial records.")

	_, _ = client.QueryTable(fmt.Sprintf("UPDATE %s SET value = \"HELLO\" WHERE id = 1;", table))
	result, _ := client.QueryTable(fmt.Sprintf("SELECT * FROM %s WHERE id = 1;", table))
	fmt.Println("After update (id=1):", result)

	_, _ = client.QueryTable(fmt.Sprintf("UPDATE %s SET value = \"new\" WHERE id = 3;", table))
	result, _ = client.QueryTable(fmt.Sprintf("SELECT * FROM %s WHERE id = 3;", table))
	fmt.Println("After update (id=3):", result)

	result, _ = client.QueryTable(fmt.Sprintf("SELECT * FROM %s WHERE id = 1;", table))
	fmt.Println("Current (id=1):", result)

	_, _ = client.QueryTable(fmt.Sprintf("DELETE FROM %s WHERE id = 1;", table))
	result, _ = client.QueryTable(fmt.Sprintf("SELECT * FROM %s WHERE id = 1;", table))
	fmt.Println("After delete (id=1):", result)

	result, _ = client.QueryTable(fmt.Sprintf("SELECT * FROM %s WHERE id = 2;", table))
	fmt.Println("Current (id=2):", result)

	fmt.Println("All operations completed successfully.")
}
