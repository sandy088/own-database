package db

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"sync"
)

type SimpleDB struct {
	mu   sync.RWMutex     // Mutex for safe concurrent access
	data map[string]int64 // In-memory index
	file *os.File         // File for persistent storage
	path string           // File path for the database
}

// OpenDB initializes or loads the database
func OpenDB(path string) (*SimpleDB, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	db := &SimpleDB{
		data: make(map[string]int64),
		file: file,
		path: path,
	}

	if err := db.loadIndex(); err != nil {
		return nil, err
	}

	return db, nil
}

// LoadIndex scans the file to build the in-memory index
func (db *SimpleDB) loadIndex() error {
	scanner := bufio.NewScanner(db.file)
	offset := int64(0)

	for scanner.Scan() {
		line := scanner.Text()
		var entry KVPair
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return err
		}

		db.data[entry.Key] = offset
		offset += int64(len(line) + 1)
	}

	return scanner.Err()
}

// Set adds or updates a key-value pair in the database
func (db *SimpleDB) Set(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	entry := KVPair{
		Key:   key,
		Value: value,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	offset, err := db.file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	if _, err := db.file.Write(append(data, '\n')); err != nil {
		return err
	}

	db.data[key] = offset
	return nil
}

// Get retrieves the value for a given key
func (db *SimpleDB) Get(key string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	offset, exists := db.data[key]
	if !exists {
		return "", errors.New("key not found")
	}

	if _, err := db.file.Seek(offset, os.SEEK_SET); err != nil {
		return "", err
	}

	reader := bufio.NewReader(db.file)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	var entry KVPair
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		return "", err
	}

	return entry.Value, nil
}

// Delete removes a key from the database
func (db *SimpleDB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	_, exists := db.data[key]
	if !exists {
		return errors.New("key not found")
	}

	delete(db.data, key)
	return nil
}

// Close ensures the file is properly closed
func (db *SimpleDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.file.Close()
}
