package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/giskook/go-toolkit"
	"github.com/giskook/gotcp"
	"github.com/giskook/gotcp/examples/das"
)

func getMacByte(Mac string) []byte {
	var mac []byte
	var macbits byte
	macbits = 0
	for i := 0; i < len(Mac)/2; i += 2 {
		macbits += Mac[i] - '0'
		macbits += (Mac[i+1] - '0') * 16
		mac = append(mac, macbits)
		macbits = 0
	}

	return mac
}

func recvNsq(q *gotcp.Mqhub, topic string, channel string) {
	config := nsq.NewConfig()
	consumer, errormsg := nsq.NewConsumer(topic, channel, config)
	if errormsg != nil {
		log.Printf("create Consumer error-->" + errormsg.Error())
		return
	}

	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		cmd := message.Body
		fmt.Printf("recv message from nsq %v\n", cmd)
		commandtype := cmd[0]
		mac := string(cmd[1:13])
		fmt.Printf("recv mac from nsq %s\n", mac)
		serialid := gktoolkit.BytesToUInt32(cmd[13:17])
		fmt.Printf("recv serialid from nsq %d\n", serialid)
		_topic := string(cmd[17:len(cmd)])
		fmt.Printf("recv topic from nsq %s\n", _topic)
		var feedback []byte
		if commandtype == 0 { // check online
			feedback = append(feedback, 0x00)
			feedback = append(feedback, mac...)
			feedback = append(feedback, gktoolkit.UInt32ToBytes(serialid)...)
			if q.Exist(mac) {
				feedback = append(feedback, 0x01)
			} else {
				feedback = append(feedback, 0x00)
			}
			q.Send(_topic, feedback)
		} else {
			macupper := strings.ToUpper(mac)
			if q.GetConn(macupper) != nil {
				fmt.Printf("-------mac :%s,topic :%s----\n", mac, _topic)
				q.GetConn(macupper).SetTopic(_topic)
				q.GetConn(macupper).SetMac(macupper)
				q.GetConn(macupper).NsqWritePacket(das.NewNsqPacket(_topic, commandtype, getMacByte(macupper), serialid, 0), time.Second)
			}
		}

		return nil
	}))

	err := consumer.ConnectToNSQD(q.GetAddr())
	if err != nil {
		log.Panic("Could not connect")
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	// create a tcp listener
	tcpAddr, err := net.ResolveTCPAddr("tcp4", ":7082")
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	// create a server
	config := &gotcp.Config{
		PacketSendChanLimit:    20,
		PacketReceiveChanLimit: 20,
	}

	mqconfig := &gotcp.MqConfig{
		Addr:    "127.0.0.1:4150",
		Topic:   "commandproduce",
		Channel: "1",
	}

	nsqhub := gotcp.Newmqhub(mqconfig, &das.NsqProtocol{})

	nsqhub.Start()
	go recvNsq(nsqhub, "command", "1")

	srv := gotcp.NewServer(config, &das.DasCallback{}, &das.DasProtocol{}, nsqhub)

	// starts service
	go srv.Start(listener, time.Second)
	fmt.Println("listening:", listener.Addr())

	// catchs system signal
	chSig := make(chan os.Signal)
	signal.Notify(chSig, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("Signal: ", <-chSig)

	// stops server
	srv.Stop()
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
