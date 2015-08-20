package gotcp

import (
	"log"
	"strings"
	"sync"

	"github.com/bitly/go-nsq"
)

type MqConfig struct {
	Addr    string
	Topic   string
	Channel string
}

type Mqhub struct {
	config    *MqConfig
	protocol  Protocol
	waitGroup *sync.WaitGroup

	producer *nsq.Producer
	//consumer *nsq.Consumer
	conns    map[uint32]*Conn
	connsmac map[string]uint32
}

func Newmqhub(config *MqConfig, protocol Protocol) *Mqhub {
	return &Mqhub{
		config:    config,
		protocol:  protocol,
		waitGroup: &sync.WaitGroup{},
		conns:     make(map[uint32]*Conn),
		connsmac:  make(map[string]uint32),
	}
}

func (q *Mqhub) Start() {
	q.waitGroup.Add(1)
	config := nsq.NewConfig()
	var errormsg error
	q.producer, errormsg = nsq.NewProducer(q.config.Addr, config)
	//q.Send(q.config.Topic, []byte("abc"))
	if errormsg != nil {
		log.Printf("create producer error")
	}
}

func (q *Mqhub) Stop() {
	q.waitGroup.Done()
	q.waitGroup.Wait()

	q.producer.Stop()
	//q.consumer.Stop()
}

func (q *Mqhub) Send(topic string, value []byte) error {
	err := q.producer.Publish(topic, value)

	return err
}

func (q *Mqhub) Exist(id string) bool {
	idupper := strings.ToUpper(id)
	_, ok := q.connsmac[idupper]

	return ok
}

func (q *Mqhub) GetConn(mac string) *Conn {

	log.Println(q.conns)
	log.Println(q.connsmac)
	return q.conns[q.connsmac[mac]]
}

func (q *Mqhub) GetAddr() string {
	return q.config.Addr
}

func (q *Mqhub) RemoveConn(index uint32, mac string) {
	delete(q.conns, index)
	delete(q.connsmac, mac)
}
