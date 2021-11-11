package standalone_storage

import (
	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type storageRead struct {
	kv  *badger.DB
	txn *badger.Txn
}

func (sr *storageRead) GetCF(cf string, key []byte) ([]byte, error) {
	v, err := engine_util.GetCF(sr.kv, cf, key)
	if err == nil {
		return v, nil
	}

	switch err {
	case badger.ErrKeyNotFound:
		return nil, nil
	default:
		return nil, err
	}
}

func (sr *storageRead) Close() {
	if sr.txn != nil {
		sr.txn.Discard()
	}
}

func (sr *storageRead) IterCF(cf string) engine_util.DBIterator {
	txn := sr.kv.NewTransaction(false)
	sr.txn = txn
	return engine_util.NewCFIterator(cf, txn)
}
