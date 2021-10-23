package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	ezgrpc "github.com/coderagr/ezqueuegrpc"
	"google.golang.org/grpc"
)

var port = ":8989"

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

	isDone := make(chan int, 1)

	var wg sync.WaitGroup

	fmt.Println("Receiving messages...")

	wg.Add(1)
	go func(isDone <-chan int, client ezgrpc.EzqueuedClient) {

		defer wg.Done()
		for {
			select {
			case d := <-isDone:
				if d == 1 {
					fmt.Println("Received 1 from main")
					return
				}
			case <-time.After(2 * time.Second):
				r, err := Dequeue(context.Background(), client, "testproducer", "queue-1000")
				if err != nil {
					fmt.Println(err)
				}
				fmt.Println("Messsage Received", r.Message)
			}
		}

	}(isDone, client)

	for {
		input := ""
		fmt.Scanln(&input)
		if input == "exit" {
			isDone <- 1
			break
		} else {
			fmt.Println("Type exit to end the program.")
		}
	}

	wg.Wait()

	fmt.Println("Done!")

}
