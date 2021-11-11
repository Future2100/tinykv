package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"

	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
)

func TestGetPutDelete(t *testing.T) {
	t.Run("put_get_delete_get", func(t *testing.T) {
		conn, err := grpc.Dial("localhost:20160", grpc.WithInsecure())
		if err != nil {
			log.Error("failed to connection storage", zap.Error(err))
			return
		}
		ctx := context.Background()
		client := tinykvpb.NewTinyKvClient(conn)
		_, err = client.RawPut(ctx, &kvrpcpb.RawPutRequest{
			Key:   []byte("hello"),
			Value: []byte("world"),
			Cf:    "default",
		})
		if err != nil {
			log.Error("failed to put hello", zap.Error(err))
		}
		resp, err := client.RawGet(context.Background(), &kvrpcpb.RawGetRequest{
			Key: []byte("hello"),
			Cf:  "default",
		})
		if err != nil {
			log.Error("failed to get hello", zap.Error(err))
			return
		}
		fmt.Println(string(resp.Value))
	})
}
