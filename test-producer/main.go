package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	ezgrpc "github.com/coderagr/ezqueuegrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var port = ":8989"
var wg sync.WaitGroup

func main() {

	if len(os.Args) == 1 {
		fmt.Println("Using default port:", port)
	} else {
		port = os.Args[1]
	}
	conn, err := grpc.Dial(port, grpc.WithInsecure())
	if err != nil {
		fmt.Println("Dial:", err)
		return
	}

	client := ezgrpc.NewEzqueuedClient(conn)

	fmt.Println("Sending messages...")

	producerCount := 8
	doneSignal := make([]chan int, producerCount)

	wg.Add(producerCount)

	for i := 1; i <= producerCount; i++ {

		doneSignal[i-1] = make(chan int, 1)

		queueName := "queue-" + strconv.Itoa(i*1000)
		go produce(doneSignal[i-1], client, "testproducer", queueName)

	}

	for {
		input := ""
		fmt.Scanln(&input)
		if input == "exit" {

			for i := 1; i <= producerCount; i++ {
				doneSignal[i-1] <- 1
			}

			break
		} else {
			fmt.Println("Type exit to end the program.")
		}
	}

	wg.Wait()

	fmt.Println("Done!")

}

func produce(isDone <-chan int, client ezgrpc.EzqueuedClient, appName, queueName string) {

	defer wg.Done()

	r, cerr := Create(context.Background(), client, appName, queueName)
	if cerr != nil {
		grpcError, _ := status.FromError(cerr)
		fmt.Println(grpcError.Message())

		if codes.AlreadyExists != grpcError.Code() {
			return
		}
	} else {
		fmt.Printf("Queue %s/%s Created with status %d\n", appName, queueName, r.Success)
	}

	for {
		select {
		case d := <-isDone:
			if d == 1 {
				fmt.Println("Received 1 from main")
				return
			}
		case t := <-time.After(time.Second):
			r, err := Enqueue(context.Background(), client, appName, queueName, t.String())
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("Messsage sent", r.Success)
		}
	}

}
