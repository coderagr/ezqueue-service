# ezqueue-service

## Introduction
ezqueue-service is a crash-tolerant fifo queue service. It supports the following operations on a queue:
   

  *  Create
  *  Enqueue
  *  Peek
  *  Dequeue

Each queue is uniquely identified by the system by **appname/queuename**  combo.

The service arbitrarily supports a maximum of 1000 queues. This number can be changed by modifying the **MaxQueues** const value in **queue.go**.

Messages are held in a fifo queue in memory, while a write-ahead log stores the Enqueue events in an append-only file. If the queue daemon crashes for any reason, the queues will be restored from head to tail. 
Further durability can be guaranteed by storing the WAL in a sepaarate HA sotrage system that has a dedicated power supply.

## Interface

The ezqueue will be compiled as a linux deamon. It supports gRPC and protocol buffers.

## Further enhancements in the making
 * TLS support between gRPC client and server
 * Delay and VisibilityTimeout implementation
 * HTTP API interface that can be used to Load Balance the input
 * With a little further effort, this service can be converted to serve as **VERY BASIC** event store. Events can be re-played from any point in the message history.
