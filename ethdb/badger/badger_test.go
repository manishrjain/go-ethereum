package badger

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/dbtest"
	"github.com/stretchr/testify/require"
)

func TestBadgerDB(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	t.Run("DatabaseSuite", func(t *testing.T) {
		dbtest.TestDatabaseSuite(t, func() ethdb.KeyValueStore {
			dir, err := ioutil.TempDir(dir, "each-test")
			require.NoError(t, err)

			db, err := New(dir, 1<<20, false)
			require.NoError(t, err)
			return db
		})
	})
}
