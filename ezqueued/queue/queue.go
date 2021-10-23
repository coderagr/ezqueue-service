package queue

import (
	"fmt"

	"github.com/google/uuid"
)

const (
	MaxQueues      = 1000
	MaxMessageSize = 256 //KB
	QueueTypeFifo  = true
	MessageIDStart = uint32(1001)
)

type Message struct {
	Value string
	Prev  *Message
	Next  *Message
}

//Type definitons
//PersistantQueue is responsible for persisting and managing the queue items
type Queue struct {
	Head              *Message //earliest message in the queue
	Tail              *Message //latest message in the queue
	AppName           string   //Should belong to an app
	Name              string   //Queue Name
	Id                string   //Unique identifier for this queue
	FifoQueue         bool     //this is true by default and the only value supported for now
	DelaySeconds      uint16   //number of seconds to delay
	VisibilityTimeout uint16   //number of milliseconds to wait after the message is enqueued and
	Count             uint16   //Number of messages curently in the queue
}

func (q *Queue) Bytes() []byte {

	return []byte(fmt.Sprintf("%v,%v,%v,%v,%v,%v", q.AppName, q.Name, q.Id, q.FifoQueue,
		q.DelaySeconds, q.VisibilityTimeout))

}

func (q *Queue) String() string {

	return fmt.Sprintf("%v,%v,%v,%v,%v,%v", q.AppName, q.Name, q.Id, q.FifoQueue,
		q.DelaySeconds, q.VisibilityTimeout)
}

func (q *Queue) Enqueue(msg string) {
	//First item being added to an empty queue
	if q.Head == nil {
		q.Head = newMessage(msg)
		q.Tail = q.Head

	} else {
		//Add it to the tail and move the tail
		q.Tail.Next = newMessage(msg)
		q.Tail.Next.Prev = q.Tail
		q.Tail = q.Tail.Next
	}

	q.Count++
}

func (q *Queue) Peek() (string, error) {

	return q.Head.Value, nil
}

func (q *Queue) DeQueue() error {

	q.Head = q.Head.Next
	q.Count--

	return nil
}

//NewQueue creates a new Queue object
func NewQueue(appName, name string, id string, delaySeconds,
	visibilityTimeout uint16) *Queue {

	if len(appName) == 0 || len(name) == 0 {
		return nil
	}

	if len(id) == 0 {
		id = uuid.NewString()
	}

	q := Queue{nil, nil, appName, name, id, true, delaySeconds, visibilityTimeout, 0}

	return &q
}

//NewMessage creates a new Message object
func newMessage(value string) *Message {

	m := new(Message)
	m.Value = value
	m.Next = new(Message)
	m.Prev = new(Message)

	return m
}
