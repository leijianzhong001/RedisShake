package types

import (
	"github.com/alibaba/RedisShake/internal/rdb/structure"
	"io"
)

type StringObject struct {
	value string
	key   string
}

// LoadFromBuffer 从指定的reader中读取一个字符串，并填充到当前对象
func (o *StringObject) LoadFromBuffer(rd io.Reader, key string, _ byte) {
	o.key = key
	o.value = structure.ReadString(rd)
}

func (o *StringObject) Rewrite() []RedisCmd {
	cmd := RedisCmd{}
	cmd = append(cmd, "set", o.key, o.value)
	return []RedisCmd{cmd}
}
