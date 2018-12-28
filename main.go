package amqper

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"time"

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
	Key      string
	Cmd      func()
	Time     int
	Finished chan bool
}

//Run start worker job in new routine
func (w *Worker) Run() {
	go func() {
		time.Sleep(time.Duration(w.Time) * time.Minute)
		w.Cmd()
		w.Finished <- true
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
func ConsumeQueue(name string, connection *Connection) {
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
			decoded := decodeMessage(name, message)
			finished := make(chan bool)
			worker := NewWorker(decoded.Key, decoded.Time, func() {
				fmt.Println("Worker executed.")
			}, finished)
			worker.Run()
			message.Ack(false)
		}
	}()
	<-forever
}

//NewWorker creates new instance of worker
func NewWorker(key string, t int, cmd func(), done chan bool) Worker {
	w := Worker{
		Key:      key,
		Time:     t,
		Cmd:      cmd,
		Finished: done,
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
