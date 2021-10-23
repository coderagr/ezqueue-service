package wal

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	q "github.com/coderagr/ezqueue-service/ezqueued/queue"
)

func init() {
	//Open the control file
	filepath := "/etc/ezqueue/ezqueue.test.config"
	w, ferr := os.ReadFile(filepath)
	if ferr != nil {
		log.Panicf("Unable to read the config file %s", filepath)
		return
	}

	if ferr = json.Unmarshal(w, &Config); ferr != nil {
		log.Panicf("Unable to read the config file %s", filepath)
		return
	}

}

var appendMutex sync.Mutex

func TestCreate(t *testing.T) {

	walInfo, err := Create("TestApp", "TestQueue", 10, 1)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	if walInfo == nil {
		t.Errorf("Want non-nil WalInfo, got nil")
		return
	}

	if walInfo.WalFile == nil {
		t.Errorf("Want non-nil WalFile, got nil")
		return
	}

	if walInfo.WalControlFile == nil {
		t.Errorf("Want non-nil WalControlFile, got nil")
		return
	}

	fileName := "TestAppTestQueue.control"
	fileInfo, ferr := os.Stat(path.Join(Config.Logspath, fileName))
	if ferr != nil {
		t.Errorf("Want %s, got %s", fileName, ferr.Error())
		return
	}

	if fileInfo.Size() == 0 {
		t.Errorf("Want non-zero file size for %s, got 0", fileName)
	}

	fileName = "TestAppTestQueue-1.wal"
	fileInfo, ferr = os.Stat(path.Join(Config.Logspath, fileName))
	if ferr != nil {
		t.Errorf("Want %s, got %s", fileName, ferr.Error())
		return
	}

	if fileInfo.Size() != 0 {
		t.Errorf("Want zero file size for %s, got non-zero", fileName)
	}

}

func fileSetup(t *testing.T) (*QueueInfo, error) {
	//BEGIN: Setup the unit test situation
	fullQueueName := "TestAppTestQueue"
	walInfo := new(QueueInfo)
	walControl := new(WalControl)
	walInfo.WalControlInfo = walControl

	walControlFile := fullQueueName + ControlFileExtn
	filePath := path.Join(Config.Logspath, walControlFile)

	//Open the control file
	w, ferr := os.ReadFile(filePath)
	if ferr != nil {
		return nil, ferr
	}

	//Read the contents of the control file into the structure
	//The control file has the latest info just before the app was terminated
	if err := json.Unmarshal(w, walControl); err != nil {
		return nil, err
	}

	//Open the files with file pointers
	walCtrlFilePtr, cerr := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0664)
	if cerr != nil {
		t.Errorf("Unable to open file %s", filePath)
		return nil, cerr
	}

	walInfo.Queue = q.NewQueue(walControl.MetaData.AppName, walControl.MetaData.Name, "",
		walControl.MetaData.DelaySeconds, walControl.MetaData.VisibilityTimeout)

	//Open the wal log file
	filePath = path.Join(Config.Logspath, walInfo.LogFileName(walControl.TailLsnFileNum))
	walFile, werr := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0664)
	if werr != nil {
		t.Errorf("Unable to open file %s", filePath)
		return nil, werr
	}

	walInfo.WalFile = walFile
	walInfo.WalControlFile = walCtrlFilePtr

	return walInfo, nil
}

func TestAppend(t *testing.T) {

	walInfo, werr := fileSetup(t)
	if werr != nil {
		t.Errorf(werr.Error())
		return
	}

	defer walInfo.WalControlFile.Close()
	defer walInfo.WalFile.Close()

	start := time.Now()
	var wg sync.WaitGroup

	for i := 1; i < 3; i++ {

		wg.Add(1)
		go func(index int) {
			appendMutex.Lock()
			defer wg.Done()
			defer appendMutex.Unlock()

			msg := fmt.Sprintf("Message Number %d. This message is a bit longer because the quick brown fox jumped over the lazy dog", walInfo.WalControlInfo.NextLsn)
			msgBytes := []byte(msg)

			nextLsn := walInfo.WalControlInfo.NextLsn
			walInfo.Append(msg)
			size := nextLsn + Sizes.GetWalItemPrefixSize() + uint64(len(msgBytes))

			if walInfo.WalControlInfo.NextLsn != size {
				t.Errorf("Want next LSN %d, got %d", size, walInfo.WalControlInfo.NextLsn)
			}
		}(i)
	}

	fmt.Printf("Waiting...\n")

	wg.Wait()

	since := time.Since(start)

	fmt.Printf("Time taken: %d\n", since.Milliseconds())
}

func TestMoveHead(t *testing.T) {

	walInfo, werr := fileSetup(t)
	if werr != nil {
		t.Errorf(werr.Error())
		return
	}

	defer walInfo.WalControlFile.Close()
	defer walInfo.WalFile.Close()

	prevHeadLsn := walInfo.WalControlInfo.HeadLsn
	prevTailLsn := walInfo.WalControlInfo.TailLsn

	err := walInfo.MoveHead()

	if err != nil {
		t.Errorf("Error my moving queue head in the WAL")
		return
	}

	if prevHeadLsn == walInfo.WalControlInfo.HeadLsn {
		t.Errorf("HeadLsn: want lsn thats not %d, got %d", prevHeadLsn, walInfo.WalControlInfo.HeadLsn)
		return
	}

	if prevTailLsn != walInfo.WalControlInfo.TailLsn {
		t.Errorf("HeadLsn: want %d, got %d", prevTailLsn, walInfo.WalControlInfo.TailLsn)
		return
	}
}
