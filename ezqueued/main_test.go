package main

import (
	"encoding/json"
	"log"
	"os"
	"path"
	"testing"
	"time"

	q "github.com/coderagr/ezqueue-service/ezqueued/queue"
	w "github.com/coderagr/ezqueue-service/ezqueued/wal"
)

func init() {
	//Open the control file
	filepath := "/etc/ezqueue/ezqueue.config"
	wb, ferr := os.ReadFile(filepath)
	if ferr != nil {
		log.Panicf("Unable to read the config file %s", filepath)
		return
	}

	if ferr = json.Unmarshal(wb, &w.Config); ferr != nil {
		log.Panicf("Unable to read the config file %s", filepath)
		return
	}

}

func fileSetup(t *testing.T) (*w.QueueInfo, error) {
	//BEGIN: Setup the unit test situation
	fullQueueName := "testproducerqueue-1000"
	walInfo := new(w.QueueInfo)
	walControl := new(w.WalControl)
	walInfo.WalControlInfo = walControl

	walControlFile := fullQueueName + w.ControlFileExtn
	filePath := path.Join(w.Config.Logspath, walControlFile)

	//Open the control file
	wb, ferr := os.ReadFile(filePath)
	if ferr != nil {
		return nil, ferr
	}

	//Read the contents of the control file into the structure
	//The control file has the latest info just before the app was terminated
	if err := json.Unmarshal(wb, walControl); err != nil {
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
	filePath = path.Join(w.Config.Logspath, walInfo.LogFileName(walControl.TailLsnFileNum))
	walFile, werr := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0664)
	if werr != nil {
		t.Errorf("Unable to open file %s", filePath)
		return nil, werr
	}

	walInfo.WalFile = walFile
	walInfo.WalControlFile = walCtrlFilePtr

	return walInfo, nil
}

func TestRecoverQueues(t *testing.T) {

	RecoverQueues()

	queueCount := len(queueInfo.queueWalInfo)

	if queueCount != 1 {
		t.Errorf("Queues: Want %d, got %d\n", 1, queueCount)
	}
}

func TestAppend(t *testing.T) {

	walInfo, werr := fileSetup(t)
	if werr != nil {
		t.Errorf(werr.Error())
		return
	}

	defer walInfo.WalControlFile.Close()
	defer walInfo.WalFile.Close()

	msg := time.Now().String()

	walInfo.Append(msg)

}
