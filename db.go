package main

import (
	"errors"
	"sync"
)

type InMemoryDatabase struct {
	data         map[string]string
	transactions []map[string]string
	mu           sync.RWMutex
}

const deletedMarker = "<deleted>"

func NewInMemoryDatabase() *InMemoryDatabase {
	return &InMemoryDatabase{
		data: make(map[string]string),
	}
}

func (db *InMemoryDatabase) Get(key string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(db.transactions) > 0 {
		for i := len(db.transactions) - 1; i >= 0; i-- {
			if value, ok := db.transactions[i][key]; ok {
				if value == deletedMarker {
					return "", errors.New("key not found")
				}
				return value, nil
			}
		}
	}

	value, ok := db.data[key]
	if !ok {
		return "", errors.New("key not found")
	}

	return value, nil
}

func (db *InMemoryDatabase) Set(key string, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.transactions) > 0 {
		db.transactions[len(db.transactions)-1][key] = value
	} else {
		db.data[key] = value
	}

	return nil
}

func (db *InMemoryDatabase) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.transactions) > 0 {
		db.transactions[len(db.transactions)-1][key] = deletedMarker
	} else {
		delete(db.data, key)
	}

	return nil
}

func (db *InMemoryDatabase) StartTransaction() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// snapshot := make(map[string]string)

	// if (len(db.transactions)) > 0 {
	// 	for key, value := range db.transactions[len(db.transactions)-1] {
	// 		snapshot[key] = value
	// 	}
	// } else {
	// 	for key, value := range db.data {
	// 		snapshot[key] = value
	// 	}
	// }

	db.transactions = append(db.transactions, make(map[string]string))

	return nil
}

func (db *InMemoryDatabase) Commit() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.transactions) == 0 {
		return errors.New("no transaction started")
	}

	if len(db.transactions) == 1 {
		for key, value := range db.transactions[len(db.transactions)-1] {
			if value == deletedMarker {
				delete(db.data, key)
				continue
			}
			db.data[key] = value
		}
	} else {
		for key, value := range db.transactions[len(db.transactions)-1] {
			db.transactions[len(db.transactions)-2][key] = value
		}
	}

	db.transactions = db.transactions[:len(db.transactions)-1]

	return nil
}

func (db *InMemoryDatabase) Rollback() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.transactions) == 0 {
		return errors.New("no transaction started")
	}

	db.transactions = db.transactions[:len(db.transactions)-1]

	return nil
}
