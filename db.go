package main

import (
	"errors"
)

type InMemoryDatabase struct {
	data         map[string]string
	transactions []map[string]string
}

const deletedMarker = "<deleted>"

var ErrKeyNotFound = errors.New("key not found")
var ErrNoTransaction = errors.New("no transaction started")

func NewInMemoryDatabase() *InMemoryDatabase {
	return &InMemoryDatabase{
		data: make(map[string]string),
	}
}

// Get gets the value associated with the given key.
func (db *InMemoryDatabase) Get(key string) (string, error) {
	if len(db.transactions) > 0 {
		for i := len(db.transactions) - 1; i >= 0; i-- {
			if value, ok := db.transactions[i][key]; ok {
				if value == deletedMarker {
					return "", ErrKeyNotFound
				}
				return value, nil
			}
		}
	}

	value, ok := db.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}

	return value, nil
}

// Set store a key-value pair in the database.
func (db *InMemoryDatabase) Set(key string, value string) error {
	if len(db.transactions) > 0 {
		db.transactions[len(db.transactions)-1][key] = value
	} else {
		db.data[key] = value
	}

	return nil
}

// Delete deletes the key-value pair associated with the given key.
func (db *InMemoryDatabase) Delete(key string) error {
	if len(db.transactions) > 0 {
		db.transactions[len(db.transactions)-1][key] = deletedMarker
	} else {
		delete(db.data, key)
	}

	return nil
}

// StartTransaction Start a new transaction. All operations within this transaction are isolated from others.
func (db *InMemoryDatabase) StartTransaction() error {
	db.transactions = append(db.transactions, make(map[string]string))

	return nil
}

// Commit commits all changes made within the current transaction to the database.
func (db *InMemoryDatabase) Commit() error {
	if len(db.transactions) == 0 {
		return ErrNoTransaction
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

// Rollback rollbacks all changes made within the current transaction and discard them
func (db *InMemoryDatabase) Rollback() error {
	if len(db.transactions) == 0 {
		return ErrNoTransaction
	}

	db.transactions = db.transactions[:len(db.transactions)-1]

	return nil
}
