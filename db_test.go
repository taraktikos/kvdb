package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInMemmoryDatabaseCommit(t *testing.T) {
	db := NewInMemmoryDatabase()

	require.NoError(t, db.Set("key1", "value1"))
	require.NoError(t, db.StartTransaction())
	require.NoError(t, db.Set("key1", "value2"))
	require.NoError(t, db.Commit())

	value, err := db.Get("key1")
	require.NoError(t, err)
	require.Equal(t, "value2", value)
}

func TestInMemmoryDatabaseRollback(t *testing.T) {
	db := NewInMemmoryDatabase()

	require.NoError(t, db.Set("key1", "value1"))
	require.NoError(t, db.StartTransaction())
	value, err := db.Get("key1")
	require.NoError(t, err)
	require.Equal(t, "value1", value)

	require.NoError(t, db.Set("key1", "value2"))
	value, err = db.Get("key1")
	require.NoError(t, err)
	require.Equal(t, "value2", value)

	require.NoError(t, db.Rollback())

	value, err = db.Get("key1")
	require.NoError(t, err)
	require.Equal(t, "value1", value)
}

func TestInMemmoryDatabaseNestedTransactions(t *testing.T) {
	db := NewInMemmoryDatabase()

	require.NoError(t, db.Set("key1", "value1"))
	require.NoError(t, db.StartTransaction())
	require.NoError(t, db.Set("key1", "value2"))
	value, err := db.Get("key1")
	require.NoError(t, err)
	require.Equal(t, "value2", value)

	require.NoError(t, db.StartTransaction())
	value, err = db.Get("key1")
	require.NoError(t, err)
	require.Equal(t, "value2", value)

	require.NoError(t, db.Delete("key1"))

	require.NoError(t, db.Commit())

	value, err = db.Get("key1")
	require.Error(t, err) // TODO: change to not found error
	require.Equal(t, "", value)

	require.NoError(t, db.Commit())

	value, err = db.Get("key1")
	require.Error(t, err) // TODO: change to not found error
	require.Equal(t, "", value)
}

func TestInMemmoryDatabaseNestedTransactionsWithRollBack(t *testing.T) {
	db := NewInMemmoryDatabase()

	require.NoError(t, db.Set("key1", "value1"))
	require.NoError(t, db.StartTransaction())
	require.NoError(t, db.Set("key1", "value2"))
	value, err := db.Get("key1")
	require.NoError(t, err)
	require.Equal(t, "value2", value)

	require.NoError(t, db.StartTransaction())
	value, err = db.Get("key1")
	require.NoError(t, err)
	require.Equal(t, "value2", value)
	require.NoError(t, db.Delete("key1"))
	require.NoError(t, db.Rollback())
	value, err = db.Get("key1")
	require.NoError(t, err)
	require.Equal(t, "value2", value)
	require.NoError(t, db.Commit())
	value, err = db.Get("key1")
	require.NoError(t, err)
	require.Equal(t, "value2", value)
}
