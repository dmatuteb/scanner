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
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("Error cargando configuración: %v", err)
	}

	db, err := sql.Open("oracle", config.OracleDSN)
	if err != nil {
		log.Fatalf("Error conectando a Oracle DB: %v", err)
	}
	defer db.Close()

	if err := connectSSH(config); err != nil {
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

	if config.SSHPort == 0 {
		config.SSHPort = 22
	}
	if config.ScanInterval == 0 {
		config.ScanInterval = 24
	}
	return &config, nil
}

func connectSSH(cfg *Config) error {
	sshConf := &ssh.ClientConfig{
		User: cfg.SSHUser,
		Auth: []ssh.AuthMethod{
			ssh.Password(cfg.SSHPassword),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	addr := fmt.Sprintf("%s:%d", cfg.SSHHost, cfg.SSHPort)
	client, err := ssh.Dial("tcp", addr, sshConf)
	if err != nil {
		return err
	}
	sshClient = client

	sftpC, err := sftp.NewClient(client)
	if err != nil {
		client.Close()
		return err
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
	typeMap := loadTypePrefixMap(db)

	walker := sftpClient.Walk(remoteDir)
	for walker.Step() {
		if err := walker.Err(); err != nil {
			log.Println("Error en walker:", err)
			continue
		}
		info := walker.Stat()
		if info.IsDir() {
			continue
		}

		name := info.Name()
		path := walker.Path()

		exists, err := fileExistsInDB(db, name)
		if err != nil {
			log.Println("Error verificando existencia:", err)
			continue
		}
		if exists {
			continue
		}

		typeID := matchPrefixToTypeID(name, typeMap)
		if typeID == 0 {
			log.Printf("No se encontró prefijo para el archivo: %s (omitido)", name)
			continue
		}

		insertFile(db, name, info.ModTime(), typeID)
		fmt.Printf("Insertado: %s [tipo_id: %d]\n", path, typeID)
	}
	return nil
}

func loadTypePrefixMap(db *sql.DB) map[string]int {
	rows, err := db.Query(`SELECT PREFIJO, ID FROM ORA_BANK.ACH_TIPO_ARCHIVO_REPROCESO`)
	if err != nil {
		log.Fatalf("Consulta fallida: %v", err)
	}
	defer rows.Close()

	typeMap := make(map[string]int)
	for rows.Next() {
		var prefix string
		var id int
		if err := rows.Scan(&prefix, &id); err != nil {
			log.Printf("Error al leer fila: %v", err)
			continue
		}
		typeMap[strings.ToLower(prefix)] = id
	}
	return typeMap
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
	err := db.QueryRow(`SELECT COUNT(1) FROM ORA_BANK.ACH_ARCHIVOS_REPROCESO WHERE ARCHIVO = :1`, path).Scan(&exists)
	return exists > 0, err
}

func insertFile(db *sql.DB, name string, modTime time.Time, typeID int) {
	_, err := db.Exec(
		`INSERT INTO ORA_BANK.ACH_ARCHIVOS_REPROCESO (ARCHIVO, ID_TIPO_ARCHIVO_REPROCESO, TIEMPO_REGISTRO) VALUES (:1, :2, :3)`,
		name, typeID, modTime,
	)
	if err != nil {
		log.Printf("Error insertando archivo %s: %v", name, err)
	}
}
