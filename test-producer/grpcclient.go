package main

import (
	"context"

	ezgrpc "github.com/coderagr/ezqueuegrpc"
)

func Create(ctx context.Context, m ezgrpc.EzqueuedClient, appName, queueName string) (*ezgrpc.ReturnStatus, error) {

	request := &ezgrpc.CreateParams{AppName: appName, QueueName: queueName, DelaySeconds: 10, VisibilityTimeout: 10}

	return m.Create(ctx, request)
}

func Enqueue(ctx context.Context, m ezgrpc.EzqueuedClient, appName, queueName, message string) (*ezgrpc.ReturnStatus, error) {

	request := &ezgrpc.EnqueueParams{AppName: appName, QueueName: queueName, Message: message}

	return m.Enqueue(ctx, request)
}
