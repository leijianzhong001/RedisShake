package structure

import (
	"github.com/alibaba/RedisShake/internal/log"
	"io"
	"strconv"
)

const (
	RDBEncInt8  = 0 // RDB_ENC_INT8
	RDBEncInt16 = 1 // RDB_ENC_INT16
	RDBEncInt32 = 2 // RDB_ENC_INT32
	RDBEncLZF   = 3 // RDB_ENC_LZF
)

// ReadString 按照字符串编码的方式读取字符串内容
func ReadString(rd io.Reader) string {
	// 获取下一个字符串的长度
	length, special, err := readEncodedLength(rd)
	if err != nil {
		log.PanicError(err)
	}
	if special {
		switch length {
		case RDBEncInt8:
			// 为 0 时：之后 8 bit 用于存储该整型。
			b := ReadInt8(rd)
			return strconv.Itoa(int(b))
		case RDBEncInt16:
			// 为 1 时：之后 16 bit 用于存储该整型。
			b := ReadInt16(rd)
			return strconv.Itoa(int(b))
		case RDBEncInt32:
			// 为 2 时：之后 32 bit 用于存储该整型。
			b := ReadInt32(rd)
			return strconv.Itoa(int(b))
		case RDBEncLZF:
			// 为3时： 压缩字符串编码
			inLen := ReadLength(rd)         // 压缩后字符串长度
			outLen := ReadLength(rd)        // 压缩前字符串长度
			in := ReadBytes(rd, int(inLen)) // 读取压缩后长度的字符串

			return lzfDecompress(in, int(outLen))
		default:
			log.Panicf("Unknown string encode type %d", length)
		}
	}
	// 如果不是11开头的特殊编码，说明是 简单长度前缀字符串方法， 直接读取指定长度的字符串
	return string(ReadBytes(rd, int(length)))
}

func lzfDecompress(in []byte, outLen int) string {
	out := make([]byte, outLen)

	i, o := 0, 0
	for i < len(in) {
		ctrl := int(in[i])
		i++
		if ctrl < 32 {
			for x := 0; x <= ctrl; x++ {
				out[o] = in[i]
				i++
				o++
			}
		} else {
			length := ctrl >> 5
			if length == 7 {
				length = length + int(in[i])
				i++
			}
			ref := o - ((ctrl & 0x1f) << 8) - int(in[i]) - 1
			i++
			for x := 0; x <= length+1; x++ {
				out[o] = out[ref]
				ref++
				o++
			}
		}
	}
	if o != outLen {
		log.Panicf("lzf decompress failed: outLen: %d, o: %d", outLen, o)
	}
	return string(out)
}
