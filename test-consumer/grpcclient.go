package main

import (
	"context"

	ezgrpc "github.com/coderagr/ezqueuegrpc"
)

func Dequeue(ctx context.Context, m ezgrpc.EzqueuedClient, appName, queueName string) (*ezgrpc.QueueItem, error) {

	request := &ezgrpc.DequeueParams{AppName: appName, QueueName: queueName}

	return m.Dequeue(ctx, request)
}

func Peek(ctx context.Context, m ezgrpc.EzqueuedClient, appName, queueName string) (*ezgrpc.QueueItem, error) {

	request := &ezgrpc.PeekParams{AppName: appName, QueueName: queueName}

	return m.Peek(ctx, request)
}
