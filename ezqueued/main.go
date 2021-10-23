package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"regexp"
	"sync"
	"time"

	e "github.com/coderagr/ezqueue-service/ezqueued/errors"
	q "github.com/coderagr/ezqueue-service/ezqueued/queue"
	u "github.com/coderagr/ezqueue-service/ezqueued/utilities"
	"github.com/coderagr/ezqueue-service/ezqueued/wal"
	ezgrpc "github.com/coderagr/ezqueuegrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type QueueInfoMap map[string]*wal.QueueInfo

//Top level data structures
type ProtQueueInfoMap struct {
	queueWalInfo QueueInfoMap
	mx           sync.Mutex
}

func (p *ProtQueueInfoMap) Get(key string) (*wal.QueueInfo, bool) {
	p.mx.Lock()
	defer p.mx.Unlock()

	w, ok := p.queueWalInfo[key]
	if !ok {
		return nil, false
	}

	return w, true
}

func (p *ProtQueueInfoMap) Set(key string, value *wal.QueueInfo) {
	p.mx.Lock()
	defer p.mx.Unlock()

	p.queueWalInfo[key] = value
}

func (p *ProtQueueInfoMap) Iter() QueueInfoMap {
	p.mx.Lock()
	defer p.mx.Unlock()

	return p.queueWalInfo
}

func NewQueueWalInfo() *ProtQueueInfoMap {

	return &ProtQueueInfoMap{queueWalInfo: make(map[string]*wal.QueueInfo, q.MaxQueues)}
}

var queueInfo = NewQueueWalInfo()

//Restore Error is invoked if restoration of stored queues or messages fail
type RestoreError struct {
	Message string
}

func (e *RestoreError) Error() string {
	return fmt.Sprintf("%v", e.Message)
}

var port = ":8989"

func main() {

	if len(os.Args) == 1 {
		fmt.Println("Using default port:", port)
	} else {
		port = os.Args[1]
	}

	log.Println("Restoring queues from storage....")

	if err := RecoverQueues(); err != nil {
		log.Fatal("Error restoring queues")
	}

	//Start the GRPC Server
	server := grpc.NewServer()
	var ezqueuedServer EzqueuedServer
	ezgrpc.RegisterEzqueuedServer(server, ezqueuedServer)

	reflection.Register(server)

	listen, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println(err)
		return
	}

	//Start a go routine that periodically saves the wal and control files
	go func() {

		for {

			<-time.After(20 * time.Second)

			fmt.Println("Saving files to disk...")

			for _, walInfo := range queueInfo.Iter() {

				walInfo.FlushControlFile()
				walInfo.FlushWalFile()
			}

			fmt.Println("Finished saving files to disk...")

		}
	}()

	log.Println("EzQueueService is ready!")
	fmt.Println("Serving requests...")
	server.Serve(listen)
}

//Create creates a new queue in the system and saves is in leveldb
func Create(appName, name string, delaySeconds, visibilityTimeout uint16) error {

	//Check for input data validity
	verr := u.IsValidCreateQueueInput(appName, name, &delaySeconds, &visibilityTimeout)
	if verr != nil {
		log.Println(verr.Error())
		return &e.Error{AppName: appName, Name: name, ErrorCode: e.INVALID_INPUT, ErrorMessage: verr.Error()}
	}

	//Check if the appname+QueName combo exists in the map
	if _, ok := queueInfo.Get(appName + name); ok {
		log.Printf("Failed to create queue %s", appName+name)
		return &e.Error{AppName: appName, Name: name, ErrorCode: e.ALREADY_EXISTS, ErrorMessage: e.ErrorAppQuenameExists}
	}

	//TODO Maybe a mutex protection here?
	//If 2 clients try to create a queue with the same appnamr+queuename combo

	walInfo, err := wal.Create(appName, name, delaySeconds, visibilityTimeout)

	if err != nil {
		log.Printf("Failed to create wal file for %s", appName+name)
		return err
	}

	queueInfo.Set(appName+name, walInfo)

	return nil
}

//EnQueue adds an items to the head
func EnQueue(appName, name, msg string) error {

	fullQueueName := appName + name

	//Make sure input data is valid
	if err := u.IsValidMessageInput(appName, name, msg); err != nil {
		log.Println(err.Error())
		return &e.Error{AppName: appName, Name: name, ErrorCode: e.INVALID_INPUT, ErrorMessage: err.Error()}
	}

	//Check if the queue exists
	if _, ok := queueInfo.Get(fullQueueName); !ok {
		return &e.Error{AppName: appName, Name: name, ErrorCode: e.QUEUE_DOES_NOT_EXIST, ErrorMessage: e.ErrorQueueDoesNotExist}
	}

	//Append to the WAL file
	walInfo, _ := queueInfo.Get(fullQueueName)
	if err := walInfo.Append(msg); err != nil {
		return &e.Error{AppName: appName, Name: name, ErrorCode: e.WAL_FILE_APPEND_FAILED, ErrorMessage: err.Error()}
	}

	return nil
}

//DeQueue removes the queue item at the tail
func DeQueue(appName, name string) (value string, err error) {

	fullQueueName := appName + name

	appQueue, ok := queueInfo.Get(fullQueueName)

	//Check if the Queue exists
	if !ok {
		return "", &e.Error{AppName: appName, Name: name, ErrorCode: e.QUEUE_DOES_NOT_EXIST, ErrorMessage: e.ErrorQueueDoesNotExist}
	}

	//Check if queue is empty
	if appQueue.Queue.Head == nil {
		return "", &e.Error{AppName: appName, Name: name, ErrorCode: e.QUEUE_EMPTY, ErrorMessage: e.ErrorQueueEmpty}
	}

	msg, err := appQueue.MoveHead()

	if err != nil {
		return "", err
	}

	//return the head
	return msg, nil
}

func Peek(appName, name string) (value string, err error) {

	fullQueueName := appName + name

	appQueue, ok := queueInfo.Get(fullQueueName)

	//Check if the Queue exists
	if !ok {
		return "", &e.Error{AppName: appName, Name: name, ErrorCode: e.QUEUE_DOES_NOT_EXIST, ErrorMessage: e.ErrorQueueDoesNotExist}
	}

	//Check if queue is empty
	if appQueue.Queue.Head == nil {
		return "", &e.Error{AppName: appName, Name: name, ErrorCode: e.QUEUE_EMPTY, ErrorMessage: e.ErrorQueueEmpty}
	}

	msg, err := appQueue.Queue.Peek()

	if err != nil {
		return "", err
	}

	//return the head
	return msg, nil
}

var wg sync.WaitGroup

func RecoverQueues() error {

	files, err := os.ReadDir(wal.Config.Logspath)
	if err != nil {
		log.Fatal(err)
	}

	comRegex, cerr := regexp.Compile(".control$")
	if cerr != nil {
		return cerr
	}

	if len(files) == 0 {
		fmt.Println("No control files found. Nothing to recover")
		return nil
	}

	for _, file := range files {

		if file.IsDir() {
			continue
		}

		loc := comRegex.FindStringIndex(file.Name())
		if loc == nil {
			continue
		}

		filePath := path.Join(wal.Config.Logspath, file.Name())

		wg.Add(1)
		go func(filePath string) {

			defer wg.Done()

			//Open the control file
			w, ferr := os.ReadFile(filePath)
			if ferr != nil {
				return
			}

			//Read the contents of the control file into the structure
			//The control file has the latest info just before the app was terminated
			walControl := new(wal.WalControl)
			if err := json.Unmarshal(w, walControl); err != nil {
				return
			}

			walInfo := new(wal.QueueInfo)
			walInfo.WalControlInfo = walControl

			wcInfo := walInfo.WalControlInfo
			fullQueueName := walControl.MetaData.AppName + walControl.MetaData.Name
			walFileNum := wcInfo.HeadLsnFileNum

			queueInfo.Set(fullQueueName, walInfo)

			walInfo.Queue = q.NewQueue(walControl.MetaData.AppName, walControl.MetaData.Name, "",
				walControl.MetaData.DelaySeconds, walControl.MetaData.VisibilityTimeout)

			messageCount := 0

			//Open the walcontrol file
			walCtrlFilePtr, cerr := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0664)
			if cerr != nil {
				msg := fmt.Sprintf("Unable to open wal file for %s", filePath)
				log.Panicf(msg)
				return
			}
			walInfo.WalControlFile = walCtrlFilePtr
			startLsn := wcInfo.HeadLsn

			for walFileNum <= wcInfo.TailLsnFileNum {

				if walInfo.WalFile != nil { //we are moving to a new file
					walInfo.WalFile.Close()

					//reset the start LSN. All WAL files have a start lsn of 0
					startLsn = 0
				}

				walFileName := walInfo.LogFileName(walFileNum)
				//Open the wal file
				wf, werr := os.OpenFile(path.Join(wal.Config.Logspath, walFileName), os.O_APPEND|os.O_RDWR, 0664)
				if werr != nil {
					return
				}
				walInfo.WalFile = wf

				//Prep to read the wal item from the file
				var itemPrefixBytes = make([]byte, wal.Sizes.GetWalItemPrefixSize())
				//first seek to lsn
				_, err := wf.Seek(int64(startLsn), 0)
				if err != nil {
					return
				}

				appQueue, _ := queueInfo.Get(fullQueueName)

				fmt.Printf("Reading messages from %s\n", wf.Name())

				for {
					//at this point get the itemprefix bytes
					_, err = wf.Read(itemPrefixBytes)
					if err == io.EOF {
						break
					} else if err != nil {
						return
					}

					item, derr := wal.DecodeWalItemPrefix(itemPrefixBytes)
					if derr != nil {
						return
					}

					_, err = wf.Read(item.Data)
					if err == io.EOF {
						break
					} else if err != nil {
						return
					}

					//Add the data to the head of the queue
					msg := string(item.Data)
					//fmt.Println(msg)
					appQueue.Queue.Enqueue(msg)
					messageCount++
				}

				walFileNum++

			}

			fmt.Printf("Recovered %d messages in %s\n", messageCount,
				wcInfo.MetaData.AppName+"/"+wcInfo.MetaData.Name)

		}(filePath)

	}

	//Wait for all the go routines to be finished
	wg.Wait()

	return nil
}
