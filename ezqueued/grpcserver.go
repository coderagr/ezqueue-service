package main

import (
	"context"

	ezgrpc "github.com/coderagr/ezqueuegrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	e "github.com/coderagr/ezqueue-service/ezqueued/errors"
)

type EzqueuedServer struct {
	ezgrpc.UnimplementedEzqueuedServer
}

func (EzqueuedServer) Create(ctx context.Context, r *ezgrpc.CreateParams) (*ezgrpc.ReturnStatus, error) {

	returnStatus := ezgrpc.ReturnStatus{Success: 0}

	if err := Create(r.AppName, r.QueueName, uint16(r.DelaySeconds), uint16(r.VisibilityTimeout)); err != nil {

		qErr := err.(*e.Error)

		if qErr.ErrorCode == e.ALREADY_EXISTS {
			grpcErr := status.Errorf(codes.AlreadyExists, qErr.ErrorMessage)
			return &returnStatus, grpcErr
		}

		return &returnStatus, err
	}

	returnStatus.Success = 1
	return &returnStatus, nil
}

func (EzqueuedServer) Enqueue(ctx context.Context, in *ezgrpc.EnqueueParams) (*ezgrpc.ReturnStatus, error) {
	returnStatus := ezgrpc.ReturnStatus{Success: 0}

	if err := EnQueue(in.AppName, in.QueueName, in.Message); err != nil {
		qErr := err.(*e.Error)

		if qErr.ErrorCode == e.QUEUE_DOES_NOT_EXIST {
			grpcErr := status.Errorf(codes.NotFound, qErr.ErrorMessage)
			return &returnStatus, grpcErr
		} else if qErr.ErrorCode == e.WAL_FILE_APPEND_FAILED {
			grpcErr := status.Errorf(codes.Internal, qErr.ErrorMessage)
			return &returnStatus, grpcErr
		}

		return &returnStatus, err
	}

	returnStatus.Success = 1
	return &returnStatus, nil
}

func (EzqueuedServer) Dequeue(ctx context.Context, in *ezgrpc.DequeueParams) (*ezgrpc.QueueItem, error) {

	message := ezgrpc.QueueItem{Message: ""}

	m, err := DeQueue(in.AppName, in.QueueName)

	if err != nil {
		qErr := err.(*e.Error)

		if qErr.ErrorCode == e.QUEUE_DOES_NOT_EXIST || qErr.ErrorCode == e.QUEUE_EMPTY {
			grpcErr := status.Errorf(codes.NotFound, qErr.ErrorMessage)
			return &message, grpcErr
		}
	}

	message.Message = m

	return &message, err

}

func (EzqueuedServer) Peek(ctx context.Context, in *ezgrpc.PeekParams) (*ezgrpc.QueueItem, error) {

	message := ezgrpc.QueueItem{Message: ""}

	m, err := Peek(in.AppName, in.QueueName)

	if err != nil {
		qErr := err.(*e.Error)

		if qErr.ErrorCode == e.QUEUE_DOES_NOT_EXIST || qErr.ErrorCode == e.QUEUE_EMPTY {
			grpcErr := status.Errorf(codes.NotFound, qErr.ErrorMessage)
			return &message, grpcErr
		}
	}

	message.Message = m

	return &message, err
}
