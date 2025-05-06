package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "path/filepath"
    "sort"
    "strings"
    "time"

    _ "github.com/sijms/go-ora/v2"
)

type Config struct {
    OracleDSN string `json:"oracle_dsn"`
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
        err := scanAndStoreFiles(db, watchDir)
        if err != nil {
            log.Printf("Scan error: %v", err)
        }
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

func scanAndStoreFiles(db *sql.DB, dir string) error {
    typeMap, err := loadTypePrefixMap(db)
    if err != nil {
        return fmt.Errorf("failed to load type prefixes: %w", err)
    }

    return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
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
        if exists {
            return nil
        }

        typeID := matchPrefixToTypeID(info.Name(), typeMap)
        if typeID == 0 {
            log.Printf("No matching prefix for file: %s (skipped)", info.Name())
            return nil
        }

        insertFile(db, info.Name(), path, info.Size(), info.ModTime(), typeID)
        fmt.Printf("Inserted: %s [type_id: %d]\n", path, typeID)
        return nil
    })
}

func loadTypePrefixMap(db *sql.DB) map[string]int {
    rows, err := db.Query(`SELECT prefix, id FROM file_types`)
    if err != nil {
        log.Fatalf("Query failed: %v", err)
    }
    defer rows.Close()

    typeMap := make(map[string]int)
    for rows.Next() {
        var prefix string
        var id int
        if err := rows.Scan(&prefix, &id); err != nil {
            log.Printf("Skipping row: %v", err)
            continue
        }
        typeMap[strings.ToLower(prefix)] = id
    }

    return typeMap
}

func matchPrefixToTypeID(filename string, typeMap map[string]int) int {
    filename = strings.ToLower(filename)

    // Sort prefixes by length descending to prioritize longest match
    prefixes := make([]string, 0, len(typeMap))
    for prefix := range typeMap {
        prefixes = append(prefixes, prefix)
    }
    sort.Slice(prefixes, func(i, j int) bool {
        return len(prefixes[i]) > len(prefixes[j])
    })

    for _, prefix := range prefixes {
        if strings.HasPrefix(filename, prefix) {
            return typeMap[prefix]
        }
    }

    return 0 // no match
}

func fileExistsInDB(db *sql.DB, path string) (bool, error) {
    var exists int
    err := db.QueryRow(`SELECT COUNT(1) FROM files WHERE path = :1`, path).Scan(&exists)
    return exists > 0, err
}

func insertFile(db *sql.DB, name, path string, size int64, modTime time.Time, typeID int) {
    _, err := db.Exec(
        `INSERT INTO files (name, path, size, mod_time, type_id) VALUES (:1, :2, :3, :4, :5)`,
        name, path, size, modTime, typeID,
    )
    if err != nil {
        log.Printf("Failed to insert file %s: %v", name, err)
    }
}
