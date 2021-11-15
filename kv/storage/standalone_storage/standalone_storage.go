package standalone_storage

import (
	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	badgerKV *badger.DB
	conf     *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	kv := engine_util.CreateDB(s.conf.DBPath, s.conf.Raft)
	s.badgerKV = kv

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
