// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package badger implements the key-value database layer based on Badger DB.
package badger

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/skl"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/pkg/errors"
)

var (
	// errMemorydbClosed is returned if a memory database was already closed at the
	// invocation of a data access operation.
	errMemorydbClosed = errors.New("database closed")

	// errMemorydbNotFound is returned if a key is requested that is not found in
	// the provided memory database.
	errMemorydbNotFound = errors.New("not found")
)

// Database is an ephemeral key-value store. Apart from basic data storage
// functionality it also supports batch writes and iterating over the keyspace in
// binary-alphabetical order.
type Database struct {
	bdb  *badger.DB
	lock sync.RWMutex
}

// New returns a wrapped map with all the required database interface methods
// implemented.
func New(dir string, cache int, readonly bool) (*Database, error) {
	opts := badger.DefaultOptions(dir).WithNumMemtables(10)
	bdb, err := badger.OpenManaged(opts)
	return &Database{bdb: bdb}, errors.Wrapf(err, "while opening Badger")
}

// Close deallocates the internal map and ensures any consecutive data access op
// failes with an error.
func (db *Database) Close() error {
	return db.bdb.Close()
}

// Has retrieves if a key is present in the key-value store.
func (db *Database) Has(key []byte) (bool, error) {
	var found bool
	rerr := db.bdb.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err == nil {
			found = true
		}
		return err
	})
	return found, rerr
}

// Get retrieves the given key if it's present in the key-value store.
func (db *Database) Get(key []byte) ([]byte, error) {
	var val []byte
	rerr := db.bdb.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	return val, rerr
}

// Put inserts the given value into the key-value store.
func (db *Database) Put(key []byte, value []byte) error {
	txn := db.bdb.NewTransactionAt(math.MaxUint64, true)
	if err := txn.Set(key, value); err != nil {
		return errors.Wrapf(err, "while setting key-value")
	}
	return txn.CommitAt(math.MaxUint64, nil)
}

// Delete removes the key from the key-value store.
func (db *Database) Delete(key []byte) error {
	txn := db.bdb.NewTransactionAt(math.MaxUint64, true)
	if err := txn.Delete(key); err != nil {
		return errors.Wrapf(err, "while deleting key")
	}
	return txn.CommitAt(math.MaxUint64, nil)
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (db *Database) NewBatch() ethdb.Batch {
	return &batch{
		db:     db,
		writes: make(map[string]keyvalue),
	}
}

type witerator struct {
	itr   *badger.Iterator
	val   []byte
	err   error
	nexts int
}

func (wi *witerator) Release() {
	if wi.itr != nil {
		wi.itr.Close()
	}
}
func (wi *witerator) Error() error { return wi.err }
func (wi *witerator) Key() []byte  { return wi.itr.Item().Key() }
func (wi *witerator) Next() bool {
	wi.nexts++
	if wi.nexts > 1 {
		// Do not call next on the first invocation.
		wi.itr.Next()
	}
	return wi.itr.Valid()
}
func (wi *witerator) Value() []byte {
	var err error
	wi.val, err = wi.itr.Item().ValueCopy(wi.val)
	if err != nil {
		wi.err = err
		wi.val = nil
	}
	return wi.val
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (db *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	txn := db.bdb.NewTransactionAt(math.MaxUint64, false)
	// No need to discard txn.

	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	itr := txn.NewIterator(opts)

	p := common.CopyBytes(prefix)
	p = append(p, start...)
	itr.Seek(p)
	return &witerator{
		itr: itr,
	}
}

// Stat returns a particular internal stat of the database.
func (db *Database) Stat(property string) (string, error) {
	return "", errors.New("unknown property")
}

// Compact is not needed. Badger executes compactions internally automatically.
// They do not need to be invoked externally.
func (db *Database) Compact(start []byte, limit []byte) error {
	return nil
}

// keyvalue is a key-value tuple tagged with a deletion field to allow creating
// memory-database write batches.
type keyvalue struct {
	key    []byte
	value  []byte
	delete bool
}

// batch is a write-only memory batch that commits changes to its host
// database when Write is called. A batch cannot be used concurrently.
type batch struct {
	db     *Database
	writes map[string]keyvalue
	size   int
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(key, value []byte) error {
	b.writes[string(key)] = keyvalue{common.CopyBytes(key), common.CopyBytes(value), false}
	b.size += len(key) + len(value)
	return nil
}

// Delete inserts the a key removal into the batch for later committing.
func (b *batch) Delete(key []byte) error {
	b.writes[string(key)] = keyvalue{common.CopyBytes(key), nil, true}
	b.size += len(key)
	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return b.size
}

// Write flushes any accumulated data to the memory database.
func (b *batch) Write() error {
	var writes []keyvalue
	for _, kv := range b.writes {
		writes = append(writes, kv)
	}
	sort.Slice(writes, func(i, j int) bool {
		return bytes.Compare(writes[i].key, writes[j].key) < 0
	})
	builder := skl.NewBuilder(1 << 10)
	for i, kv := range writes {
		vs := y.ValueStruct{
			Value: kv.value,
		}
		if kv.delete {
			vs.Meta = 1 << 0 // From value.go in Badger.
		}
		builder.Add(y.KeyWithTs(kv.key, math.MaxUint64), vs)
		fmt.Printf("[%d] Write key: %q value: %q\n", i, kv.key, kv.value)
	}
	sl := builder.Skiplist()

	// TODO: Add a callback to know that we have written the skiplist to disk.
	// So, we can safely shut down Geth if needed.
	return b.db.bdb.HandoverSkiplist(sl, nil)
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.writes = make(map[string]keyvalue)
	b.size = 0
}

// Replay replays the batch contents.
func (b *batch) Replay(w ethdb.KeyValueWriter) error {
	for _, keyvalue := range b.writes {
		if keyvalue.delete {
			if err := w.Delete(keyvalue.key); err != nil {
				return err
			}
			continue
		}
		if err := w.Put(keyvalue.key, keyvalue.value); err != nil {
			return err
		}
	}
	return nil
}
