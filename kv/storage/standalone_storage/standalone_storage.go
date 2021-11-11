package standalone_storage

import (
	"fmt"

	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	badgerKV   *badger.DB
	badgerOpts badger.Options
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = fmt.Sprintf("%s/data", conf.DBPath)

	return &StandAloneStorage{
		badgerOpts: opts,
	}
}

func (s *StandAloneStorage) Start() error {
	badgerKV, err := badger.Open(s.badgerOpts)
	if err != nil {
		return err
	}

	s.badgerKV = badgerKV

	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.badgerKV.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &storageRead{kv: s.badgerKV}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	return s.badgerKV.Update(func(txn *badger.Txn) error {
		for _, m := range batch {
			switch m.Data.(type) {
			case storage.Put:
				key := m.KeyWithCf()
				if err := txn.Set(key, m.Data.(storage.Put).Value); err != nil {
					return err
				}
			case storage.Delete:
				key := m.KeyWithCf()
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
		}

		return nil
	})
}
