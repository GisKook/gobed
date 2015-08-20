package das

import (
	"fmt"
	"time"

	"github.com/giskook/go-toolkit"
	"github.com/giskook/gotcp"
)

var (
	endTag   = byte(0xED)
	hexTable = []string{
		"00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0A", "0B", "0C", "0D", "0E", "0F", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "1A", "1B", "1C", "1D", "1E", "1F", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "2A", "2B", "2C", "2D", "2E", "2F", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3A", "3B", "3C", "3D", "3E", "3F", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "4A", "4B", "4C", "4D", "4E", "4F", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "5A", "5B", "5C", "5D", "5E", "5F", "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6A", "6B", "6C", "6D", "6E", "6F", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "7A", "7B", "7C", "7D", "7E", "7F", "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "8A", "8B", "8C", "8D", "8E", "8F", "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9A", "9B", "9C", "9D", "9E", "9F", "A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "AA", "AB", "AC", "AD", "AE", "AF", "B0", "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "BA", "BB", "BC", "BD", "BE", "BF", "C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "CA", "CB", "CC", "CD", "CE", "CF", "D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "DA", "DB", "DC", "DD", "DE", "DF", "E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "EA", "EB", "EC", "ED", "EE", "EF", "F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "FA", "FB", "FC", "FD", "FE", "FF",
	}
)

// Packet
type DasPacket struct {
	cmdtype byte
	data    []byte
}

func (p *DasPacket) Serialize() []byte {
	// 0xBA command feedback 0xBB heartbeat 0xBC login
	daspacket := p
	command := daspacket.GetData()
	commandtype := daspacket.GetType()
	var feedback []byte
	switch commandtype {
	case 0xBA:
		fmt.Printf("recv should up the result%x \n", command)
	case 0xAB:
		feedback = append(feedback, 0xAB)
		feedback = append(feedback, command...)
		feedback = append(feedback, endTag)
	case 0xAC:
		feedback = append(feedback, 0xAC)
		feedback = append(feedback, command...)
		feedback = append(feedback, endTag)
	default:
		gktoolkit.Trace()
	}

	return feedback
}

func (p *DasPacket) GetType() byte {
	return p.cmdtype
}

func (p *DasPacket) GetData() []byte {
	return p.data
}

func NewDasPacket(cmdtype byte, data []byte) *DasPacket {
	return &DasPacket{
		cmdtype: cmdtype,
		data:    data,
	}
}

type DasProtocol struct {
}

func (this *DasProtocol) ReadPacket(goconn *gotcp.Conn) (gotcp.Packet, error) {
	conn := goconn.GetRawConn()
	for {
		data := make([]byte, 1024)
		readLengh, err := conn.Read(data)

		if err != nil { // EOF, or worse
			return nil, err
		}

		if readLengh == 0 { // Connection maybe cloased by the client
			return nil, gotcp.ErrConnClosing
		} else {
			goconn.GetRecvBytes().Write(data[:readLengh])
			fmt.Println(goconn.GetRecvBytes().Bytes())
			if goconn.GetRecvBytes().Bytes()[0] == 0xBA ||
				goconn.GetRecvBytes().Bytes()[0] == 0xBB ||
				goconn.GetRecvBytes().Bytes()[0] == 0xBC {
				if goconn.GetRecvBytes().Len() >= 8 {
					cmdtype, _ := goconn.GetRecvBytes().ReadByte()
					if cmdtype == 0xBA {
						result := goconn.GetRecvBytes().Next(6) // cmdtype + result + serialid
						goconn.GetRecvBytes().Next(1)
						return NewDasPacket(0xBA, result), nil
					} else if cmdtype == 0xBB {
						mac := goconn.GetRecvBytes().Next(6)
						goconn.GetRecvBytes().Next(1)
						return NewDasPacket(0xBB, mac), nil
					} else if cmdtype == 0xBC {
						mac := goconn.GetRecvBytes().Next(6)
						goconn.GetRecvBytes().Next(1)
						return NewDasPacket(0xBC, mac), nil
					}
				} else {
					return nil, gotcp.ErrReadHalf
				}
			} else {
				goconn.GetRecvBytes().Reset()
				return nil, gotcp.ErrReadHalf
			}
		}
	}
}

type DasCallback struct {
}

func (this *DasCallback) OnConnect(c *gotcp.Conn) bool {
	addr := c.GetRawConn().RemoteAddr()
	c.PutExtraData(addr)
	fmt.Println("OnConnect:", addr)

	return true
}

func getMac(Mac []byte) string {
	var mac string
	for i := 0; i < len(Mac); i++ {
		mac += hexTable[Mac[i]]
	}

	fmt.Println("==================================mac : ==============", mac)
	return mac
}

func (this *DasCallback) OnMessage(c *gotcp.Conn, p gotcp.Packet) bool {
	// 0xBA command feedback 0xBB heartbeat 0xBC login
	// 0xBA cmdtype(1-8) status(0/1) serialid
	daspacket := p.(*DasPacket)
	command := daspacket.GetData()
	commandtype := daspacket.GetType()
	fmt.Println("----onmessage ", command)
	switch commandtype {
	case 0xBA:
		var result []byte
		var cmdop byte
		cmdop = command[0]*10 + command[1]
		result = append(result, cmdop)
		result = append(result, c.GetMac()...)
		result = append(result, command[3:7]...)
		result = append(result, command[2])
		c.Send(c.GetTopic(), result)

		fmt.Printf("-----recv should up the result%x \n", result)
	case 0xBB:
		c.SetTimeFlag(time.Now().Unix())
		c.AsyncWritePacket(NewDasPacket(0xAB, command), time.Second)
	case 0xBC:
		c.SetID(getMac(command), c.GetIndex())
		c.AsyncWritePacket(NewDasPacket(0xAC, command), time.Second)
	default:
		gktoolkit.Trace()
	}

	return true
}

func (this *DasCallback) OnClose(c *gotcp.Conn) {
	fmt.Println("OnClose:", c.GetExtraData())
}
