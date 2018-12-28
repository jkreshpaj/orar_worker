package amqpworker

import (
	"encoding/json"
	"log"
	"reflect"
	"time"
	"strings"

	"github.com/streadway/amqp"
)

var typeRegistry = make(map[string]reflect.Type)

//Appointment message type from appointment queue
type Appointment struct {
	Key    string `json:"key"`
	Time   int    `json:"time"`
	Status string `json:"status"`
	For    string `json:"for"`
	UserID string `json:"userId"`
}

//Worker type contains worker properties
type Worker struct {
	Key	string
	Message	Appointment
	Cmd	func()
	Time	int
	Data	chan interface{}
}

//Run start worker job in new routine
func (w *Worker) Run() {
	go func() {
		time.Sleep(time.Duration(w.Time) * time.Minute)
		w.Cmd()
		w.Data <- w.Message
	}()
}

//Connection contains amqp connection and channel
type Connection struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
}

//CreateConnection returns amqp connection and channel
func CreateConnection(URI string) (*Connection, error) {
	makeTypes()
	conn := new(Connection)
	amqpConn, err := amqp.Dial(URI)
	if err != nil {
		return nil, err
	}
	conn.Connection = amqpConn
	amqpChan, err := conn.Connection.Channel()
	if err != nil {
		return nil, err
	}
	conn.Channel = amqpChan

	return conn, nil
}

//ConsumeQueue consumes a queue and
func ConsumeQueue(name string, connection *Connection, queueCmd func(), data chan interface{}) {
	log.Println("{CONSUMING QUEUE}:", name)
	messages, _ := connection.Channel.Consume(
		name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	forever := make(chan bool)
	go func() {
		for message := range messages {
			decoded := decodeMessage(strings.Split(name, ".")[0], message)
			worker := NewWorker(decoded, queueCmd, data)
			worker.Run()
			message.Ack(false)
		}
	}()
	<-forever
}

//NewWorker creates new instance of worker
func NewWorker(message Appointment, cmd func(), data chan interface{}) Worker {
	w := Worker{
		Key:		message.Key,
		Time:		message.Time,
		Message:	message,
		Cmd:		cmd,
		Data:		data,
	}
	return w
}

func decodeMessage(name string, message amqp.Delivery) Appointment {
	decoded := reflect.New(typeRegistry[name]).Elem().Interface().(Appointment)
	err := json.Unmarshal([]byte(message.Body), &decoded)
	if err != nil {
		log.Fatal("Error unmarshaling message", err)
	}
	return decoded
}

func makeTypes() {
	typeRegistry["test"] = reflect.TypeOf(Appointment{})
	typeRegistry["appointment"] = reflect.TypeOf(Appointment{})
}
