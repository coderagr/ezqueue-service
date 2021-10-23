package errors

import "fmt"

//Errors definitions
const (
	ALREADY_EXISTS = iota
	QUEUE_EMPTY
	QUEUE_DOES_NOT_EXIST
	WAL_FILE_DOES_NOT_EXIST
	WAL_CONTROL_FILE_DOES_NOT_EXIST
	WAL_FILE_FAILED_TO_CREATE
	WAL_CONTROL_FILE_FAILED_TO_CREATE
	WAL_FILE_APPEND_FAILED
	WAL_CONTROL_SAVE_FAILED
	INVALID_INPUT
)

const (
	ErrorExceedsMaxQueueSize = "Message exceeds max queue size"
	ErrorAppQuenameExists    = "The application and queue combo already exists"
	ErrorQueueDoesNotExist   = "The application and queue combo does not exist"
	ErrorQueueEmpty          = "Empty"
	ErrorInvalidInput        = "Input was either empty or not valid"
)

//QueueError stores info about an error that occurs during creation of a queue
type Error struct {
	AppName      string //Application name
	Name         string //Queue name
	ErrorCode    int
	ErrorMessage string //messsage that describes what went wrong
}

func (e *Error) Error() string {
	return fmt.Sprintf("%v.%v: %v", e.AppName, e.Name, e.ErrorMessage)
}
