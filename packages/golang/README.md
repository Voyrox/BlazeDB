# XeonDB Go driver
This package provides a Go driver for XeonDB, allowing you to connect to a XeonDB instance and execute SQL queries from your Go applications.

## Highlights
- **Lightweight and efficient**: Uses only the Go standard library.
- **Full SQL support**: Execute any SQL command that XeonDB recognizes.
- **Easy connection management**: Simple API for connecting, disconnecting, and handling connection errors.
- **Concurrent-safe requests**: Multiple goroutines can issue queries; responses are matched FIFO.
- **Comprehensive error handling**: Clear errors for bad JSON, disconnects, and server-side failures.

## Commands supported
Please refer to the [documentation](https://voyrox.github.io/Xeondb/) for a complete list of supported SQL commands and their syntax. The Go driver supports executing any SQL command that XeonDB recognizes.

## queryTable vs query

- QueryTable: This method is specifically designed for executing SQL statements that interact with tables. It is optimized for handling operations such as creating tables, inserting data, selecting records, updating existing entries, and deleting records. The `QueryTable` method prints results in a structured ASCII table format.
  - Example usage of `QueryTable`:
    ```go
    result, _ := client.QueryTable("SELECT * FROM my_table;")
    fmt.Println(result)
    ```

- Query: This method is a more general-purpose function that can be used for executing any SQL statement, including those that may not directly interact with tables (e.g., administrative commands, configuration changes). The `Query` method returns the decoded JSON response.
  - Example usage of `Query`:
    ```go
    result, _ := client.Query("PING;")
    fmt.Println(result)
    ```

## Install

```bash
go get github.com/Voyrox/Xeondb/packages/golang@latest
```

## Example usage
```go
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
```

## Authentication

Server-side (config snippet):

```yml
auth:
  username: admin
  password: change-me
```

Client-side:

```go
client := xeondbdriver.NewXeondbClient(xeondbdriver.Options{Host: host, Port: port})
if !client.Connect() {
    panic("connect failed")
}

// If you want to auth explicitly
res, err := client.Auth("admin", "change-me")
fmt.Println(res, err)
```

## Devnotes

### Run before publishing
```bash
go test ./...
```

### Publish changes
This is a Go module. Publishing is done by pushing a git tag (Go tooling fetches tagged versions).

```bash
git tag v0.1.0
git push origin v0.1.0
```
