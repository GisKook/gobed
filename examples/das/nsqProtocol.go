package das

import (
	"bytes"
	"fmt"

	"github.com/giskook/go-toolkit"
	"github.com/giskook/gotcp"
)

type NsqPacket struct {
	topic    string
	cmdtype  byte
	mac      []byte
	serialID uint32
	result   byte
}

func (this *NsqPacket) Serialize() []byte {
	// 0：is online 1：left up 2：left down 3：all up
	// 4：all down 5：back up 6：back down 7：leg up 8：leg down
	nsqpacket := this
	cmdtype := nsqpacket.cmdtype
	var feedback []byte
	feedback = append(feedback, 0xAA)
	switch cmdtype {
	case 10:
		feedback = append(feedback, 0x01)
		feedback = append(feedback, 0x00)
	case 11:
		feedback = append(feedback, 0x01)
		feedback = append(feedback, 0x01)
	case 21:
		feedback = append(feedback, 0x02)
		feedback = append(feedback, 0x01)
	case 20:
		feedback = append(feedback, 0x02)
		feedback = append(feedback, 0x00)
	case 31:
		feedback = append(feedback, 0x03)
		feedback = append(feedback, 0x01)
	case 30:
		feedback = append(feedback, 0x03)
		feedback = append(feedback, 0x00)
	case 41:
		feedback = append(feedback, 0x04)
		feedback = append(feedback, 0x01)
	case 40:
		feedback = append(feedback, 0x04)
		feedback = append(feedback, 0x00)
	case 51:
		feedback = append(feedback, 0x05)
		feedback = append(feedback, 0x01)
	case 50:
		feedback = append(feedback, 0x05)
		feedback = append(feedback, 0x00)
	case 61:
		feedback = append(feedback, 0x06)
		feedback = append(feedback, 0x01)
	case 60:
		feedback = append(feedback, 0x06)
		feedback = append(feedback, 0x00)
	case 71:
		feedback = append(feedback, 0x07)
		feedback = append(feedback, 0x01)
	case 70:
		feedback = append(feedback, 0x07)
		feedback = append(feedback, 0x00)
	case 81:
		feedback = append(feedback, 0x08)
		feedback = append(feedback, 0x01)
	case 80:
		feedback = append(feedback, 0x08)
		feedback = append(feedback, 0x00)
	}
	feedback = append(feedback, gktoolkit.UInt32ToBytes(this.serialID)...)
	feedback = append(feedback, endTag)

	return feedback
}

func NewNsqPacket(topic string, cmdtype byte, mac []byte, serialID uint32, result byte) *NsqPacket {
	return &NsqPacket{
		topic:    topic,
		cmdtype:  cmdtype,
		mac:      mac,
		serialID: serialID,
		result:   result,
	}
}

type NsqProtocol struct {
}

func (this *NsqProtocol) ReadPacket(goconn *gotcp.Conn) (gotcp.Packet, error) {

	conn := goconn.GetRawConn()
	fullBuf := bytes.NewBuffer([]byte{})
	fmt.Println("Read packet")
	for {
		data := make([]byte, 1024)
		readLengh, err := conn.Read(data)

		if err != nil { // EOF, or worse
			return nil, err
		}

		if readLengh == 0 { // Connection maybe cloased by the client
			return nil, gotcp.ErrConnClosing
		} else {
			fullBuf.Write(data[:readLengh])
			cmdtype, err := fullBuf.ReadByte()
			if err != nil {
				return nil, err
			}
			if cmdtype == 0xBA {
				result := fullBuf.Next(7)
				//			end := fullBuf.Next(1)
				return NewDasPacket(0xBA, result), nil
			} else if cmdtype == 0xBB {
				mac := fullBuf.Next(6)
				//		end := fullBuf.Next(1)
				return NewDasPacket(0xBB, mac), nil
			} else if cmdtype == 0xBC {
				mac := fullBuf.Next(6)
				//	end := fullBuf.Next(1)
				return NewDasPacket(0xBC, mac), nil
			}
		}
	}
}
