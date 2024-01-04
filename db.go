package main

import "errors"

type InMemmoryDatabase struct {
	data         map[string]string
	transactions []map[string]string
}

const deletedMarker = "<deleted>"

func NewInMemmoryDatabase() *InMemmoryDatabase {
	return &InMemmoryDatabase{
		data:         make(map[string]string),
		transactions: make([]map[string]string, 0),
	}
}

// Get returns the value for the given key.
func (db *InMemmoryDatabase) Get(key string) (string, error) {
	for i := len(db.transactions) - 1; i >= 0; i-- {
		value, ok := db.transactions[i][key]
		if ok {
			if value == deletedMarker {
				return "", errors.New("key not found")
			}
			return value, nil
		}
	}
	value, ok := db.data[key]
	if !ok || value == deletedMarker {
		return "", errors.New("key not found")
	}
	return value, nil
}

// Set sets the value for the given key.
func (db *InMemmoryDatabase) Set(key string, value string) error {
	if len(db.transactions) > 0 {
		db.transactions[len(db.transactions)-1][key] = value
	} else {
		db.data[key] = value
	}
	return nil
}

// Delete the key-value pair associated with the given key.
func (db *InMemmoryDatabase) Delete(key string) error {
	if len(db.transactions) > 0 {
		db.transactions[len(db.transactions)-1][key] = deletedMarker
	} else {
		db.data[key] = deletedMarker
	}
	return nil
}

// Start a new transaction. All operations within this transaction are isolated from others.
func (db *InMemmoryDatabase) StartTransaction() error {
	db.transactions = append(db.transactions, make(map[string]string))
	return nil
}

// Commit all changes made within the current transaction to the database.
func (db *InMemmoryDatabase) Commit() error {
	if len(db.transactions) > 0 {
		transaction := db.transactions[len(db.transactions)-1]
		if len(db.transactions) > 1 {
			previousTransaction := db.transactions[len(db.transactions)-2]
			for key, value := range transaction {
				if value == deletedMarker {
					if _, ok := previousTransaction[key]; ok {
						delete(previousTransaction, key)
					}
				} else {
					previousTransaction[key] = value
				}
			}
		} else {
			for key, value := range transaction {
				if value == deletedMarker {
					if _, ok := db.data[key]; ok {
						delete(db.data, key)
					}
				} else {
					db.data[key] = value
				}
			}
		}
		db.transactions = db.transactions[:len(db.transactions)-1]
	}
	return nil
}

// Roll back all changes made within the current transaction and discard them.
func (db *InMemmoryDatabase) Rollback() error {
	if len(db.transactions) > 0 {
		db.transactions = db.transactions[:len(db.transactions)-1]
	}
	return nil
}
