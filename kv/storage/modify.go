package storage

import "github.com/pingcap-incubator/tinykv/kv/util/engine_util"

// Modify is a single modification to TinyKV's underlying storage.
type Modify struct {
	Data interface{}
}

type Put struct {
	Key   []byte
	Value []byte
	Cf    string
}

type Delete struct {
	Key []byte
	Cf  string
}

func (m *Modify) Key() []byte {
	switch m.Data.(type) {
	case Put:
		return m.Data.(Put).Key
	case Delete:
		return m.Data.(Delete).Key
	}
	return nil
}

func (m *Modify) Value() []byte {
	if putData, ok := m.Data.(Put); ok {
		return putData.Value
	}

	return nil
}

func (m *Modify) Cf() string {
	switch m.Data.(type) {
	case Put:
		return m.Data.(Put).Cf
	case Delete:
		return m.Data.(Delete).Cf
	}
	return ""
}

func (m *Modify) KeyWithCf() []byte {
	switch m.Data.(type) {
	case Put:
		put := m.Data.(Put)
		return engine_util.KeyWithCF(put.Cf, put.Key)
	case Delete:
		d := m.Data.(Delete)
		return engine_util.KeyWithCF(d.Cf, d.Key)
	}

	return nil
}

func NewDelete(key []byte, cf string) Modify {
	return Modify{
		Data: Delete{
			Key: key,
			Cf:  cf,
		},
	}
}

func NewPut(key []byte, value []byte, cf string) Modify {
	return Modify{
		Data: Put{
			Key:   key,
			Value: value,
			Cf:    cf,
		},
	}
}
