package wal

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"sync"

	q "github.com/coderagr/ezqueue-service/ezqueued/queue"
)

//Data structures to store WAL related information for a particular queue
//Walitems are stores in the wal files
//Wal Control has information about the instantaneous state of the wal file

type QueueInfo struct {
	WalFile        *os.File
	WalControlFile *os.File
	WalControlInfo *WalControl
	Queue          *q.Queue

	queueAccessMutex sync.Mutex
}

func (w *QueueInfo) LogFileName(walFileNum uint64) string {

	return w.WalControlInfo.MetaData.AppName + w.WalControlInfo.MetaData.Name + "-" + strconv.FormatUint(walFileNum, 10) + Logsextn

}

/*
	MoveHead method advances the Head lsn by one item in the walfile and saves the current position the control file
*/
func (w *QueueInfo) MoveHead() (string, error) {

	w.queueAccessMutex.Lock()
	defer w.queueAccessMutex.Unlock()

	walFileNum := w.WalControlInfo.HeadLsnFileNum

	var nextLsn uint64
	var err error
	var fileSize int64

	msg := w.Queue.Head.Value

	//Case where head and tail are in the same file
	if w.WalControlInfo.TailLsnFileNum == walFileNum {

		//Same file, single item in the queue
		if w.WalControlInfo.HeadLsn == w.WalControlInfo.TailLsn {
			w.Queue.DeQueue()
			return msg, nil
		}

		//Head is behind tail. So move it one item ahead
		nextLsn, _, err = w.getNextLsn(walFileNum)
		if err != nil {
			return "", err
		}

	} else { //Case where tail is in another file with a higher file number

		nextLsn, fileSize, err = w.getNextLsn(walFileNum)
		if err != nil {
			return "", err
		}

		if nextLsn > uint64(fileSize) { //We have reached the end of the file. Move to the next file
			nextLsn = 0 //lsn for a new file always starts at 0
			w.WalControlInfo.HeadLsnFileNum = walFileNum + 1
		}
	}

	w.WalControlInfo.HeadLsn = nextLsn

	//Save the new control file values for this queue
	if err := w.saveControlFile(); err != nil {
		return "", err
	}

	///Now we are ready to dequeue from the actual queue
	w.Queue.DeQueue()

	return msg, nil
}

func (w *QueueInfo) getNextLsn(walFileNum uint64) (uint64, int64, error) {
	fileName := w.LogFileName(walFileNum)
	filePath := path.Join(Config.Logspath, fileName)

	f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return 0, 0, err
	}

	defer f.Close()
	fileInfo, err := f.Stat()
	s := fileInfo.Size()

	//Seek to the current head
	_, serr := f.Seek(int64(w.WalControlInfo.HeadLsn), 0)

	if serr != nil {
		return 0, 0, err
	}

	//Prep to read the wal item at the currrent head
	var itemPrefixBytes = make([]byte, Sizes.GetWalItemPrefixSize())

	//at this point get the itemprefix bytes
	_, err = f.Read(itemPrefixBytes)
	if err != nil {
		return 0, 0, err
	}

	//Decode the current head
	item, derr := DecodeWalItemPrefix(itemPrefixBytes)
	if derr != nil {
		return 0, 0, derr
	}

	//Calculate the next lsn and set it in the control ds
	return w.WalControlInfo.HeadLsn + Sizes.GetWalItemPrefixSize() + item.Size, s, nil
}

func (w *QueueInfo) saveControlFile() error {

	//START: Push wal control info to disk
	walc, err := json.Marshal(w.WalControlInfo)
	if err != nil {
		return err
	}

	//Empty out the current file
	ferr := w.WalControlFile.Truncate(0)
	if ferr != nil {
		return ferr
	}

	//Seek to the begining of the file
	//Write the latest walcontrolinfo into the file
	w.WalControlFile.Seek(0, 0)
	if _, err := w.WalControlFile.Write(walc); err != nil {
		return err
	}

	return nil
}

func (w *QueueInfo) FlushControlFile() error {
	w.queueAccessMutex.Lock()
	defer w.queueAccessMutex.Unlock()

	if err := w.WalControlFile.Sync(); err != nil {
		log.Printf("Error saving %s: %s", w.WalControlFile.Name(), err.Error())
		return err
	}

	return nil
}

func (w *QueueInfo) FlushWalFile() error {
	w.queueAccessMutex.Lock()
	defer w.queueAccessMutex.Unlock()

	if err := w.WalFile.Sync(); err != nil {
		log.Printf("Error saving %s: %s", w.WalFile.Name(), err.Error())
		return err
	}

	return nil
}

func (w *QueueInfo) saveWalItem(itemBytes []byte) error {

	//stores the total offset including itself
	var walItemBytes []byte
	walItemBytes = append(walItemBytes, itemBytes...)

	_, err := w.WalFile.Write(walItemBytes)
	if err != nil {
		return err
	}

	fileInfo, _ := w.WalFile.Stat()
	s := fileInfo.Size()
	fmt.Println("File Size", s)

	return nil
}

func (w *QueueInfo) segmentWalFile() error {

	if w.WalControlInfo.NextLsn >= MaxFileSize {
		//Cose the current file
		w.WalFile.Close()

		//increment the walfile number
		w.WalControlInfo.TailLsnFileNum++
		fileName := w.LogFileName(w.WalControlInfo.TailLsnFileNum)
		fullPath := path.Join(Config.Logspath, fileName)
		fptr, err := os.OpenFile(fullPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println(err)
			return err
		}

		w.WalFile = fptr
		w.WalControlInfo.NextLsn = 0
	}

	return nil
}

func (w *QueueInfo) Append(msg string) error {

	//Protect this whole function from another go routine that is trying to enqueue into the same unique queue
	w.queueAccessMutex.Lock()
	defer w.queueAccessMutex.Unlock()

	fmt.Println("Saving", msg)

	//Build a message object
	msgBytes := []byte(msg)

	//update the filenum if it exceeds the file size
	if err := w.segmentWalFile(); err != nil {
		return err
	}

	item := WalItem{w.WalControlInfo.NextLsn, ENQUEUE, w.WalControlInfo.TailLsnFileNum, uint64(len(msgBytes)), msgBytes}

	size := Sizes.GetWalItemPrefixSize() + uint64(len(msgBytes))

	itemBytes, err := EncodeWalItem(item, size)
	if err != nil {
		return err
	}

	w.WalControlInfo.TailLsn = w.WalControlInfo.NextLsn

	//size of the file until previous block will be the lsn of the next wal item
	w.WalControlInfo.NextLsn += size

	//save the control file
	err = w.saveControlFile()
	if err != nil {
		return err
	}

	//append the wal item
	err = w.saveWalItem(itemBytes)
	if err != nil {
		return err
	}

	w.Queue.Enqueue(msg)

	return nil
}

type QueueMetaData struct {
	AppName           string `json:"appname"`
	Name              string `json:"queueName"`
	DelaySeconds      uint16 `json:"delayseconds"`
	VisibilityTimeout uint16 `json:"visibilitytimeout"`
}

type WalControl struct {
	HeadLsn        uint64        `json:"headlsn"`
	HeadLsnFileNum uint64        `json:"headlsnfilenum"`
	TailLsn        uint64        `json:"taillsn"`
	TailLsnFileNum uint64        `json:"taillsnfilenum"`
	NextLsn        uint64        `json:"nextlsn"`
	MetaData       QueueMetaData `json:"queuemetadata"`
}
