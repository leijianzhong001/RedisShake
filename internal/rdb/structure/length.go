package structure

import (
	"encoding/binary"
	"fmt"
	"github.com/alibaba/RedisShake/internal/log"
	"io"
)

const (
	RDB6ByteLen  = 0 // RDB_6BITLEN
	RDB14ByteLen = 1 // RDB_14BITLEN
	len32or64Bit = 2
	lenSpecial   = 3 // RDB_ENCVAL
	RDB32ByteLen = 0x80
	RDB64ByteLen = 0x81
)

// ReadLength 读取长度。即按照整数编码的方式读取整形值或字符串长度
func ReadLength(rd io.Reader) uint64 {
	length, special, err := readEncodedLength(rd)
	if special {
		log.Panicf("illegal length special=true, encoding: %d", length)
	}
	if err != nil {
		log.PanicError(err)
	}
	return length
}

// readEncodedLength 获取编码长度
func readEncodedLength(rd io.Reader) (length uint64, special bool, err error) {
	var lengthBuffer = make([]byte, 8)
	// 读取一个字节，这个字节里隐藏了当前字符串的具体编码方式
	// 		- **<font color=00ff00>简单的长度前缀编码字符；</font>**
	// 		- **<font color=00ff00>使用字符串编码整型；</font>**
	// 		- **<font color=00ff00>压缩字符串；</font>**
	firstByte := ReadByte(rd)
	// 0xc0 => 11000000 与运算，把后面的6 bit都变成0，然后右移6位，取前两个字节
	first2bits := (firstByte & 0xc0) >> 6 // first 2 bits of encoding
	switch first2bits {
	case RDB6ByteLen:
		// 00 如果高位以 00 开始：当前 byte 剩余 6 bit 表示一个整数。
		// 0x3f => 00111111
		// firstByte & 0x3f 就是取firstByte后六位的值
		length = uint64(firstByte) & 0x3f
	case RDB14ByteLen:
		// 01 如果高位以 01 开始：当前 byte 剩余 6 bit，加上接下来的 8 bit 表示一个整数。
		nextByte := ReadByte(rd)
		// 取后6个bit,然后左移八位，给nextByte腾地方
		// 以实际长度783为例，firstByte为 01000011，nextByte为 00001111
		// uint64(firstByte)                              ==>  00000000 00000000 00000000 00000000 00000000 00000000 00000000 01000011
		// (uint64(firstByte)&0x3f)                       ==>  00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000011
		// (uint64(firstByte)&0x3f)<<8                    ==>  00000000 00000000 00000000 00000000 00000000 00000000 00000011 00000000
		// (uint64(firstByte)&0x3f)<<8 | uint64(nextByte) ==>  00000000 00000000 00000000 00000000 00000000 00000000 00000011 00001111
		length = (uint64(firstByte)&0x3f)<<8 | uint64(nextByte)
	case len32or64Bit:
		// 10 如果高位以 10 开始：忽略当前 byte 剩余的 6 bit，接下来的 4 byte 表示一个整数。
		if firstByte == RDB32ByteLen {
			_, err = io.ReadFull(rd, lengthBuffer[0:4])
			if err != nil {
				return 0, false, fmt.Errorf("read len32Bit failed: %s", err.Error())
			}
			length = uint64(binary.BigEndian.Uint32(lengthBuffer))
		} else if firstByte == RDB64ByteLen {
			_, err = io.ReadFull(rd, lengthBuffer)
			if err != nil {
				return 0, false, fmt.Errorf("read len64Bit failed: %s", err.Error())
			}
			length = binary.BigEndian.Uint64(lengthBuffer)
		} else {
			return 0, false, fmt.Errorf("illegal length encoding: %x", firstByte)
		}
	case lenSpecial:
		// 11 如果高位以 11 开始：特殊编码格式，剩余 6 bit 用于表示该格式, 剩余6位为：
		// 		- 为 0 时：之后 8  bit 用于存储该整型。
		//		- 为 1 时：之后 16 bit 用于存储该整型。
		//		- 为 2 时：之后 32 bit 用于存储该整型。
		special = true
		length = uint64(firstByte) & 0x3f
	}
	return length, special, nil
}
