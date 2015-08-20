package gotcp

import (
	"bytes"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Error type
var (
	ErrConnClosing   = errors.New("use of closed network connection")
	ErrWriteBlocking = errors.New("write packet was blocking")
	ErrReadBlocking  = errors.New("read packet was blocking")
	ErrReadHalf      = errors.New("read half packet")
)

// Conn exposes a set of callbacks for the various events that occur on a connection
type Conn struct {
	srv               *Server
	conn              *net.TCPConn  // the raw connection
	extraData         interface{}   // to save extra data
	closeOnce         sync.Once     // close the conn, once, per instance
	closeFlag         int32         // close flag
	closeChan         chan struct{} // close chanel
	packetSendChan    chan Packet   // packet send chanel
	packetReceiveChan chan Packet   // packeet receive chanel

	packetNsqReceiveChan chan Packet // packet receive nsq chanel
	//cmdbufferChan        chan byte
	recieveBuffer *bytes.Buffer

	index    uint32
	topic    string
	mac      string
	timeflag int64
	ticker   *time.Ticker
}

// ConnCallback is an interface of methods that are used as callbacks on a connection
type ConnCallback interface {
	// OnConnect is called when the connection was accepted,
	// If the return value of false is closed
	OnConnect(*Conn) bool

	// OnMessage is called when the connection receives a packet,
	// If the return value of false is closed
	OnMessage(*Conn, Packet) bool

	// OnClose is called when the connection closed
	OnClose(*Conn)
}

// newConn returns a wrapper of raw conn
func newConn(conn *net.TCPConn, srv *Server, index uint32) *Conn {
	return &Conn{
		srv:                  srv,
		conn:                 conn,
		closeChan:            make(chan struct{}),
		packetSendChan:       make(chan Packet, srv.config.PacketSendChanLimit),
		packetReceiveChan:    make(chan Packet, srv.config.PacketReceiveChanLimit),
		packetNsqReceiveChan: make(chan Packet, 64),
		//cmdbufferChan:        make(chan byte, 1024),
		recieveBuffer: bytes.NewBuffer([]byte{}),

		index:    index,
		timeflag: time.Now().Unix(),
		ticker:   time.NewTicker(60),
	}
}

// GetExtraData gets the extra data from the Conn
func (c *Conn) GetExtraData() interface{} {
	return c.extraData
}

// PutExtraData puts the extra data with the Conn
func (c *Conn) PutExtraData(data interface{}) {
	c.extraData = data
}

func (c *Conn) GetRecvBytes() *bytes.Buffer {
	return c.recieveBuffer
}

// GetRawConn returns the raw net.TCPConn from the Conn
func (c *Conn) GetRawConn() *net.TCPConn {
	return c.conn
}

// Close closes the connection
func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		atomic.StoreInt32(&c.closeFlag, 1)
		close(c.closeChan)
		close(c.packetSendChan)
		close(c.packetReceiveChan)
		close(c.packetNsqReceiveChan)
		//	close(c.cmdbufferChan)
		c.conn.Close()
		c.srv.mqhub.RemoveConn(c.index, c.mac)
		c.srv.callback.OnClose(c)
	})
}

// IsClosed indicates whether or not the connection is closed
func (c *Conn) IsClosed() bool {
	return atomic.LoadInt32(&c.closeFlag) == 1
}

func (c *Conn) GetMac() string {
	return c.mac
}

func (c *Conn) SetMac(mac string) {
	c.mac = mac
}

func (c *Conn) GetTopic() string {
	return c.topic
}

func (c *Conn) SetTopic(topic string) {
	c.topic = topic
}

func (c *Conn) Send(topic string, value []byte) bool {
	c.srv.mqhub.Send(topic, value)

	return true
}

// AsyncReadPacket async reads a packet, this method will never block
func (c *Conn) AsyncReadPacket(timeout time.Duration) (Packet, error) {
	if c.IsClosed() {
		return nil, ErrConnClosing
	}

	if timeout == 0 {
		select {
		case p := <-c.packetReceiveChan:
			return p, nil

		default:
			return nil, ErrReadBlocking
		}

	} else {
		select {
		case p := <-c.packetReceiveChan:
			return p, nil

		case <-c.closeChan:
			return nil, ErrConnClosing

		case <-time.After(timeout):
			return nil, ErrReadBlocking
		}
	}
}

// AsyncWritePacket async writes a packet, this method will never block
func (c *Conn) AsyncWritePacket(p Packet, timeout time.Duration) error {
	if c.IsClosed() {
		return ErrConnClosing
	}

	if timeout == 0 {
		select {
		case c.packetSendChan <- p:
			return nil

		default:
			return ErrWriteBlocking
		}

	} else {
		select {
		case c.packetSendChan <- p:
			return nil

		case <-c.closeChan:
			return ErrConnClosing

		case <-time.After(timeout):
			return ErrWriteBlocking
		}
	}
}

func (c *Conn) NsqWritePacket(p Packet, timeout time.Duration) error {
	if c.IsClosed() {
		return ErrConnClosing
	}

	if timeout == 0 {
		select {
		case c.packetNsqReceiveChan <- p:
			return nil

		default:
			return ErrWriteBlocking
		}

	} else {
		select {
		case c.packetNsqReceiveChan <- p:
			return nil

		case <-c.closeChan:
			return ErrConnClosing

		case <-time.After(timeout):
			return ErrWriteBlocking
		}
	}
}

func (c *Conn) SetID(mac string, index uint32) error {
	c.srv.mqhub.connsmac[mac] = index

	return nil
}

func (c *Conn) GetIndex() uint32 {
	return c.index
}

func (c *Conn) SetTimeFlag(timeflag int64) {
	c.timeflag = timeflag
}

// Do it
func (c *Conn) Do() {
	if !c.srv.callback.OnConnect(c) {
		return
	}

	go c.handleLoop()
	go c.readLoop()
	go c.writeLoop()
	go c.writeToclientLoop()
	go c.checkHeart()
}

func (c *Conn) readLoop() {
	c.srv.waitGroup.Add(1)
	defer func() {
		recover()
		c.Close()
		c.srv.waitGroup.Done()
	}()

	for {
		select {
		case <-c.srv.exitChan:
			return

		case <-c.closeChan:
			return

		default:
		}

		p, err := c.srv.protocol.ReadPacket(c)

		if err != nil && err != ErrReadHalf {
			return
		}

		if err != ErrReadHalf {
			c.packetReceiveChan <- p
		}
	}
}

func (c *Conn) writeLoop() {
	c.srv.waitGroup.Add(1)
	defer func() {
		recover()
		c.Close()
		c.srv.waitGroup.Done()
	}()

	for {
		select {
		case <-c.srv.exitChan:
			return

		case <-c.closeChan:
			return

		case p := <-c.packetSendChan:
			if _, err := c.conn.Write(p.Serialize()); err != nil {
				return
			}
		}
	}
}

func (c *Conn) handleLoop() {
	c.srv.waitGroup.Add(1)
	defer func() {
		recover()
		c.Close()
		c.srv.waitGroup.Done()
	}()

	for {
		select {
		case <-c.srv.exitChan:
			return

		case <-c.closeChan:
			return

		case p := <-c.packetReceiveChan:
			if !c.srv.callback.OnMessage(c, p) {
				return
			}
		}
	}
}

func (c *Conn) writeToclientLoop() {
	c.srv.waitGroup.Add(1)
	defer func() {
		recover()
		c.Close()
		c.srv.waitGroup.Done()
	}()

	for {
		select {
		case <-c.srv.exitChan:
			return

		case <-c.closeChan:
			return

		case p := <-c.packetNsqReceiveChan:
			if _, err := c.conn.Write(p.Serialize()); err != nil {
				return
			}
		}
	}
}

func (c *Conn) checkHeart() {
	c.srv.waitGroup.Add(1)
	defer func() {
		recover()
		c.Close()
		c.srv.waitGroup.Done()
	}()
	for {
		<-c.ticker.C
		now := time.Now().Unix()
		if now-c.timeflag > 600 {
			return
		}
	}
}
