package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/alibaba/RedisShake/internal/config"
	"github.com/alibaba/RedisShake/internal/entry"
	"github.com/alibaba/RedisShake/internal/log"
	"github.com/alibaba/RedisShake/internal/rdb/structure"
	"github.com/alibaba/RedisShake/internal/rdb/types"
	"github.com/alibaba/RedisShake/internal/statistics"
	"github.com/alibaba/RedisShake/internal/utils"
	"io"
	"os"
	"strconv"
	"time"
)

const (
	kFlagFunction2 = 245  // function library data
	kFlagFunction  = 246  // old function library data for 7.0 rc1 and rc2
	kFlagModuleAux = 247  // Module auxiliary data.
	kFlagIdle      = 0xf8 // LRU idle time.
	kFlagFreq      = 0xf9 // LFU frequency.
	kFlagAUX       = 0xfa // RDB aux field.
	kFlagResizeDB  = 0xfb // Hash table resize hint.
	kFlagExpireMs  = 0xfc // Expire time in milliseconds.
	kFlagExpire    = 0xfd // Old expire time in seconds.
	kFlagSelect    = 0xfe // DB number of the following keys.
	kEOF           = 0xff // End of the RDB file.
)

type Loader struct {
	replStreamDbId int // https://github.com/alibaba/RedisShake/pull/430#issuecomment-1099014464

	nowDBId  int
	expireMs int64
	idle     int64
	freq     int64

	filPath string
	fp      *os.File

	ch         chan *entry.Entry
	dumpBuffer bytes.Buffer
}

func NewLoader(filPath string, ch chan *entry.Entry) *Loader {
	ld := new(Loader)
	ld.ch = ch
	ld.filPath = filPath
	return ld
}

func (ld *Loader) ParseRDB() int {
	var err error
	ld.fp, err = os.OpenFile(ld.filPath, os.O_RDONLY, 0666)
	if err != nil {
		log.Panicf("open file failed. file_path=[%s], error=[%s]", ld.filPath, err)
	}
	defer func() {
		err = ld.fp.Close()
		if err != nil {
			log.Panicf("close file failed. file_path=[%s], error=[%s]", ld.filPath, err)
		}
	}()
	// bufio分段读取，不会将整个文件加载到内存中
	rd := bufio.NewReader(ld.fp)
	//magic + version 即REDIS + 0006
	buf := make([]byte, 9)
	_, err = io.ReadFull(rd, buf)
	if err != nil {
		log.PanicError(err)
	}
	// 校验REDIS魔数
	if !bytes.Equal(buf[:5], []byte("REDIS")) {
		log.Panicf("verify magic string, invalid file format. bytes=[%v]", buf[:5])
	}
	// 获取redis版本 0009
	version, err := strconv.Atoi(string(buf[5:]))
	if err != nil {
		log.PanicError(err)
	}
	log.Infof("RDB version: %d", version)

	// read entries
	ld.parseRDBEntry(rd)

	// force update rdb_sent_size for issue: https://github.com/alibaba/RedisShake/issues/485
	fi, err := os.Stat(ld.filPath)
	if err != nil {
		log.Panicf("NewRDBReader: os.Stat error: %s", err.Error())
	}
	statistics.Metrics.RdbSendSize = uint64(fi.Size())
	return ld.replStreamDbId
}

func (ld *Loader) parseRDBEntry(rd *bufio.Reader) {
	// for stat
	UpdateRDBSentSize := func() {
		offset, err := ld.fp.Seek(0, io.SeekCurrent)
		if err != nil {
			log.PanicError(err)
		}
		statistics.UpdateRDBSentSize(uint64(offset))
	}
	defer UpdateRDBSentSize()
	// read one entry 一秒给tick通道发送一个时间戳
	tick := time.Tick(time.Second * 1)
	for true {
		typeByte := structure.ReadByte(rd)
		switch typeByte {
		case kFlagIdle:
			// 0xF8 LRU redis key的LRU时间戳
			ld.idle = int64(structure.ReadLength(rd))
		case kFlagFreq:
			// 0xF9 LFU LFU频率
			ld.freq = int64(structure.ReadByte(rd))
		case kFlagAUX:
			// redis元属性 0xfa
			// structure.ReadString的含义因该是按照rdb的字符串编码方式，读取一个字符串
			key := structure.ReadString(rd)
			value := structure.ReadString(rd)
			if key == "repl-stream-db" {
				var err error
				ld.replStreamDbId, err = strconv.Atoi(value)
				if err != nil {
					log.PanicError(err)
				}
				log.Infof("RDB repl-stream-db: %d", ld.replStreamDbId)
			} else if key == "lua" {
				// redis 7 ?
				e := entry.NewEntry()
				e.Argv = []string{"script", "load", value}
				e.IsBase = true
				ld.ch <- e
				log.Infof("LUA script: [%s]", value)
			} else {
				log.Infof("RDB AUX fields. key=[%s], value=[%s]", key, value)
			}
		case kFlagResizeDB:
			// 0xFB RESIZEDB  描述 key 数目和设置了过期时间 key 数目
			dbSize := structure.ReadLength(rd)
			expireSize := structure.ReadLength(rd)
			log.Infof("RDB resize db. db_size=[%d], expire_size=[%d]", dbSize, expireSize)
		case kFlagExpireMs:
			// 0xFC EXPIRETIMEMS key过期时间，使用毫秒表示。
			ld.expireMs = int64(structure.ReadUint64(rd)) - time.Now().UnixMilli()
			if ld.expireMs < 0 {
				ld.expireMs = 1
			}
		case kFlagExpire:
			// 0xFD EXPIRETIME  key-过期时间，使用秒表示。
			ld.expireMs = int64(structure.ReadUint32(rd))*1000 - time.Now().UnixMilli()
			if ld.expireMs < 0 {
				ld.expireMs = 1
			}
		case kFlagSelect:
			// 0xFE SELECTDB 选库标识，后面紧跟数据库编号
			ld.nowDBId = int(structure.ReadLength(rd))
		case kEOF:
			// 0xFF EOF rdb文件结束符
			return
		default:
			// value的类型标识 OBJECT_TYPE 已经在前面被读取到 typeByte 中了
			// 读取一个key
			key := structure.ReadString(rd)
			var value bytes.Buffer
			// io.TeeReader返回一个Reader，它将从reader(rd)中读取的内容写入writer(&value)。
			// 通过它执行的所有从reader(rd)中读取的操作都与相应的对writer(&value)的写入操作相匹配。没有内部缓冲——写入操作必须在读取操作完成之前完成。 写入时遇到的任何错误都将报告为读错误。
			anotherReader := io.TeeReader(rd, &value)
			o := types.ParseObject(anotherReader, typeByte, key)
			// 本次value的值大于 512mb
			if uint64(value.Len()) > config.Config.Advanced.TargetRedisProtoMaxBulkLen {
				// 如果值大于512mb，将命令改为对应的redis api, 如string就是set
				cmds := o.Rewrite()
				for _, cmd := range cmds {
					e := entry.NewEntry()
					e.IsBase = true
					e.DbId = ld.nowDBId
					e.Argv = cmd
					ld.ch <- e
				}
				if ld.expireMs != 0 {
					e := entry.NewEntry()
					e.IsBase = true
					e.DbId = ld.nowDBId
					e.Argv = []string{"PEXPIRE", key, strconv.FormatInt(ld.expireMs, 10)}
					ld.ch <- e
				}
			} else {
				e := entry.NewEntry()
				e.IsBase = true
				e.DbId = ld.nowDBId
				// 这里的意思应该是将的渠道的value值转为dump以后的序列化形式，然后通过restore命令加载到redis内存中
				v := ld.createValueDump(typeByte, value.Bytes())
				// RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
				e.Argv = []string{"restore", key, strconv.FormatInt(ld.expireMs, 10), v} // 10代表10进制
				if config.Config.Advanced.RDBRestoreCommandBehavior == "rewrite" {
					if config.Config.Target.Version < 3.0 {
						log.Panicf("RDB restore command behavior is rewrite, but target redis version is %f, not support REPLACE modifier", config.Config.Target.Version)
					}
					e.Argv = append(e.Argv, "replace")
				}
				if ld.idle != 0 && config.Config.Target.Version >= 5.0 {
					e.Argv = append(e.Argv, "idletime", strconv.FormatInt(ld.idle, 10))
				}
				if ld.freq != 0 && config.Config.Target.Version >= 5.0 {
					e.Argv = append(e.Argv, "freq", strconv.FormatInt(ld.freq, 10))
				}
				ld.ch <- e
			}
			// 复位
			ld.expireMs = 0
			ld.idle = 0
			ld.freq = 0
		}
		select {
		case <-tick:
			UpdateRDBSentSize()
		default:
		}
	}
}

// createValueDump创建value的dump字符串 以便restore到redis中
// dump命令解释：dump命令以redis特定的格式序列化存储在key处的值，并将其返回给用户。返回值可以使用RESTORE命令合成回Redis key。
// 序列化格式是不透明和非标准的，但是它有一些语义特征:它包含一个64位校验和，用于确保检测到错误。 RESTORE命令确保在使用序列化的值合成键之前检查校验和。
// 值的编码格式与RDB使用的格式相同。RDB版本被编码在序列化的值中，因此不同的Redis版本与不兼容的RDB格式将拒绝处理序列化的值。
// 序列化的值不包含过期信息。为了获取当前值的生存时间，应该使用ptl命令。如果key不存在，则返回nil大容量回复。
func (ld *Loader) createValueDump(typeByte byte, val []byte) string {
	ld.dumpBuffer.Reset()
	// value类型
	_, _ = ld.dumpBuffer.Write([]byte{typeByte})
	// value
	_, _ = ld.dumpBuffer.Write(val)
	// binary.Write将数据的二进制形式写入writer
	// uint16(6) ==> 00000000 00000110
	// 这里如果写入的是rdb版本的话，是不是不应该写死6
	_ = binary.Write(&ld.dumpBuffer, binary.LittleEndian, uint16(6))
	// calc crc
	sum64 := utils.CalcCRC64(ld.dumpBuffer.Bytes())
	// 写入校验和
	_ = binary.Write(&ld.dumpBuffer, binary.LittleEndian, sum64)
	return ld.dumpBuffer.String()
}
