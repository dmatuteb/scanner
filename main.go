package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "path/filepath"
    "time"

    _ "github.com/sijms/go-ora/v2"
)

type Config struct {
    OracleDSN string `json:"oracle_dsn"` // Format: oracle://user:pass@host:port/service_name
}

const (
    configFile   = "config.json"
    watchDir     = "./watch_folder"
    scanInterval = 24 * time.Hour
)

func main() {
    config, err := loadConfig(configFile)
    if err != nil {
        log.Fatalf("Failed to load config: %v", err)
    }

    db, err := sql.Open("oracle", config.OracleDSN)
    if err != nil {
        log.Fatalf("Failed to connect to Oracle DB: %v", err)
    }
    defer db.Close()

    for {
        scanAndStoreFiles(db, watchDir)
        time.Sleep(scanInterval)
    }
}

func loadConfig(filename string) (*Config, error) {
    data, err := os.ReadFile(filename)
    if err != nil {
        return nil, err
    }

    var config Config
    err = json.Unmarshal(data, &config)
    if err != nil {
        return nil, err
    }

    return &config, nil
}

func scanAndStoreFiles(db *sql.DB, dir string) {
    err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }

        if info.IsDir() {
            return nil
        }

        exists, err := fileExistsInDB(db, path)
        if err != nil {
            return err
        }

        if !exists {
            insertFile(db, info.Name(), path, info.Size(), info.ModTime())
            fmt.Printf("New file added to DB: %s\n", path)
        }

        return nil
    })

    if err != nil {
        log.Printf("Error scanning directory: %v", err)
    }
}

func fileExistsInDB(db *sql.DB, path string) (bool, error) {
    var exists int
    query := `SELECT COUNT(1) FROM files WHERE path = :1`
    err := db.QueryRow(query, path).Scan(&exists)
    return exists > 0, err
}

func insertFile(db *sql.DB, name, path string, size int64, modTime time.Time) {
    query := `INSERT INTO files (name, path, size, mod_time) VALUES (:1, :2, :3, :4)`
    _, err := db.Exec(query, name, path, size, modTime)
    if err != nil {
        log.Printf("Failed to insert file: %v", err)
    }
}
