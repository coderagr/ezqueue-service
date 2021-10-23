package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"reflect"

	q "github.com/coderagr/ezqueue-service/ezqueued/queue"
)

const (
	Logsextn        = ".wal"
	ControlFileExtn = ".control"
	MaxFileSize     = 20000 //bytes
)

type WalConfig struct {
	Logspath string `json:"logspath"`
}

var Config = WalConfig{}

func init() {
	filepath := "/etc/ezqueue/ezqueue.config"
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

//Each wal type can be an Enqueue, Dequeue or Delete.
type WalType uint64

const (
	ENQUEUE WalType = 0
	DEQUEUE WalType = 1
	DELETE  WalType = 2
)

type WalItemPrefix struct {
	Lsn  uint64
	Size uint64
}

type WalItem struct {
	Lsn        uint64
	ItemType   WalType
	WalFileNum uint64
	Size       uint64
	Data       []byte
}

func Create(appName, queueName string, delay, visibilityTimeout uint16) (*QueueInfo, error) {

	fullQueueName := appName + queueName

	//Create the wal info and control structures
	walInfo := new(QueueInfo)
	walControl := new(WalControl)
	walInfo.WalControlInfo = walControl

	walControl.MetaData.AppName = appName
	walControl.MetaData.Name = queueName
	walControl.MetaData.DelaySeconds = delay
	walControl.MetaData.VisibilityTimeout = visibilityTimeout

	walControl.TailLsnFileNum = uint64(1)
	walControl.HeadLsn = 0
	walControl.HeadLsnFileNum = walControl.TailLsnFileNum
	walControl.TailLsn = 0

	walc, err := json.Marshal(&walControl)
	if err != nil {
		return nil, err
	}

	//create the wal control file
	walControlFile := fullQueueName + ControlFileExtn
	filePath := path.Join(Config.Logspath, walControlFile)
	err = os.WriteFile(filePath, walc, 0644)
	if err != nil {
		return nil, err
	}

	//open the walfile control file
	walCtrlFilePtr, cerr := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0664)
	if cerr != nil {
		msg := fmt.Sprintf("Unable to open wal file for %s", filePath)
		log.Panicf(msg)
		return nil, &FileError{Message: msg}
	}

	//create the wal log file
	filePath = path.Join(Config.Logspath, walInfo.LogFileName(walControl.TailLsnFileNum))
	walFile, werr := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0664)
	if werr != nil {
		msg := fmt.Sprintf("Unable to open wal file for %s", walInfo.LogFileName(walControl.TailLsnFileNum))
		log.Panicf(msg)
		return nil, &FileError{Message: msg}
	}

	walInfo.WalFile = walFile
	walInfo.WalControlFile = walCtrlFilePtr

	walInfo.Queue = q.NewQueue(appName, queueName, "", delay, visibilityTimeout)

	return walInfo, nil
}

func EncodeWalItem(item WalItem, size uint64) ([]byte, error) {

	buf := make([]byte, size)

	binary.LittleEndian.PutUint64(buf[0:], uint64(item.Lsn))
	binary.LittleEndian.PutUint64(buf[8:], uint64(item.ItemType))
	binary.LittleEndian.PutUint64(buf[16:], item.WalFileNum)
	binary.LittleEndian.PutUint64(buf[24:], item.Size)

	copy(buf[32:], item.Data)

	return buf, nil
}

func DecodeWalItemPrefix(itemPrefix []byte) (WalItem, error) {

	walItem := WalItem{}

	walItem.Lsn = binary.LittleEndian.Uint64(itemPrefix[0:])
	walItem.ItemType = WalType(binary.LittleEndian.Uint64(itemPrefix[8:]))
	walItem.WalFileNum = binary.LittleEndian.Uint64(itemPrefix[16:])
	walItem.Size = binary.LittleEndian.Uint64(itemPrefix[24:])
	walItem.Data = make([]byte, walItem.Size)

	return walItem, nil
}

type TypeSizes struct {
	IntSize           uint64
	WalItemPrefixSize uint64
}

var Sizes *TypeSizes = new(TypeSizes)

func (ts *TypeSizes) GetIntSize() uint64 {

	if ts.IntSize == 0 {
		ts.IntSize = uint64(reflect.TypeOf(ts.IntSize).Size())
	}

	return ts.IntSize
}

func (ts *TypeSizes) GetWalItemPrefixSize() uint64 {

	if ts.WalItemPrefixSize == 0 {
		ts.WalItemPrefixSize = uint64(ts.GetIntSize() * 4)
	}

	return ts.WalItemPrefixSize
}

type FileError struct {
	Message string
}

func (e *FileError) Error() string {
	return fmt.Sprintf("%v", e.Message)
}
