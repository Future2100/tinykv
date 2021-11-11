package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(ctx context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	storageReader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	bytesValue, err := storageReader.GetCF(req.GetCf(), req.GetKey())
	if err == nil {
		return &kvrpcpb.RawGetResponse{Value: bytesValue, NotFound: bytesValue == nil}, nil
	}

	return nil, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	err := server.storage.Write(nil, []storage.Modify{
		storage.NewPut(req.Key, req.Value, req.Cf),
	})
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	err := server.storage.Write(nil, []storage.Modify{
		storage.NewDelete(req.Key, req.Cf),
	})
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	storageReader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	iter := storageReader.IterCF(req.GetCf())
	defer iter.Close()

	kvPairs := make([]*kvrpcpb.KvPair, 0)
	iter.Seek(req.GetStartKey())
	for iter.Valid() {
		item := iter.Item()

		value, err := item.Value()
		if err != nil {
			continue
		}

		kvPairs = append(kvPairs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})

		if len(kvPairs) == int(req.GetLimit()) {
			break
		}

		iter.Next()
	}

	return &kvrpcpb.RawScanResponse{Kvs: kvPairs}, nil
}
