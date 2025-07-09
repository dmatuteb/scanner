package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/pkg/sftp"
	_ "github.com/sijms/go-ora/v2"
	"golang.org/x/crypto/ssh"
)

type Config struct {
	OracleDSN    string `json:"oracle_dsn"`
	SSHUser      string `json:"ssh_user"`
	SSHPassword  string `json:"ssh_password"`
	SSHHost      string `json:"ssh_host"`
	SSHPort      int    `json:"ssh_port"`
	WatchDir     string `json:"watch_dir"`
	ScanInterval int    `json:"scan_interval_hours"`
}

var (
	sshClient  *ssh.Client
	sftpClient *sftp.Client
)

func main() {
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("Error cargando configuración: %v", err)
	}

	db, err := sql.Open("oracle", config.OracleDSN)
	if err != nil {
		log.Fatalf("Error conectando a la base de datos Oracle: %v", err)
	}
	defer db.Close()

	if err := connectSSH(config.SSHUser, config.SSHPassword, config.SSHHost, config.SSHPort); err != nil {
		log.Fatalf("Error conectando vía SSH: %v", err)
	}
	defer closeSSH()

	interval := time.Duration(config.ScanInterval) * time.Hour

	for {
		err := scanAndStoreFiles(db, config.WatchDir)
		if err != nil {
			log.Printf("Error durante escaneo: %v", err)
		}
		time.Sleep(interval)
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

	// Valores por defecto si no vienen en el JSON
	if config.SSHPort == 0 {
		config.SSHPort = 22
	}
	if config.WatchDir == "" {
		config.WatchDir = "./watch_folder"
	}
	if config.ScanInterval == 0 {
		config.ScanInterval = 24
	}

	return &config, nil
}

func connectSSH(user, password, host string, port int) error {
	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	addr := fmt.Sprintf("%s:%d", host, port)
	client, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return fmt.Errorf("no se pudo conectar al servidor SSH: %w", err)
	}
	sshClient = client

	sftpC, err := sftp.NewClient(client)
	if err != nil {
		sshClient.Close()
		return fmt.Errorf("no se pudo crear cliente SFTP: %w", err)
	}
	sftpClient = sftpC
	return nil
}

func closeSSH() {
	if sftpClient != nil {
		sftpClient.Close()
	}
	if sshClient != nil {
		sshClient.Close()
	}
}

func scanAndStoreFiles(db *sql.DB, remoteDir string) error {
	typeMap, err := loadTypePrefixMap(db)
	if err != nil {
		return fmt.Errorf("no se pudieron cargar los prefijos de tipo: %w", err)
	}

	walker := sftpClient.Walk(remoteDir)
	for walker.Step() {
		if err := walker.Err(); err != nil {
			return err
		}

		fi := walker.Stat()
		if fi.IsDir() {
			continue
		}

		path := walker.Path()
		exists, err := fileExistsInDB(db, path)
		if err != nil {
			return err
		}
		if exists {
			continue
		}

		typeID := matchPrefixToTypeID(fi.Name(), typeMap)
		if typeID == 0 {
			log.Printf("No se encontró prefijo coincidente para el archivo: %s (omitido)", fi.Name())
			continue
		}

		modTime := fi.ModTime()
		size := fi.Size()

		insertFile(db, fi.Name(), path, size, modTime, typeID)
		fmt.Printf("Insertado: %s [tipo_id: %d]\n", path, typeID)
	}
	return nil
}

func loadTypePrefixMap(db *sql.DB) (map[string]int, error) {
	rows, err := db.Query(`SELECT prefix, id FROM file_types`)
	if err != nil {
		return nil, fmt.Errorf("error en consulta: %w", err)
	}
	defer rows.Close()

	typeMap := make(map[string]int)
	for rows.Next() {
		var prefix string
		var id int
		if err := rows.Scan(&prefix, &id); err != nil {
			log.Printf("Fila omitida: %v", err)
			continue
		}
		typeMap[strings.ToLower(prefix)] = id
	}

	return typeMap, nil
}

func matchPrefixToTypeID(filename string, typeMap map[string]int) int {
	filename = strings.ToLower(filename)

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

	return 0
}

func fileExistsInDB(db *sql.DB, path string) (bool, error) {
	var exists int
	err := db.QueryRow(`SELECT COUNT(1) FROM files WHERE path = :1`, path).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}

func insertFile(db *sql.DB, name, path string, size int64, modTime time.Time, typeID int) {
	_, err := db.Exec(
		`INSERT INTO files (name, path, size, mod_time, type_id) VALUES (:1, :2, :3, :4, :5)`,
		name, path, size, modTime, typeID,
	)
	if err != nil {
		log.Printf("Error insertando archivo %s: %v", name, err)
	}
}
