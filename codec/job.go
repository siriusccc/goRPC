package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

// Job 定义Job类型, 是Codec接口的一个实现, 对通过网络发送和接收的数据进行序列化和反序列化
type Job struct {
	conn   io.ReadWriteCloser
	buff   *bufio.Writer
	decode *gob.Decoder // gob解码器
	encode *gob.Encoder // gob编码器
}

// 确保某个类型实现了某个接口的所有方法
// 将空值 nil 转换为 *Job 类型，再转换为 Codec 接口，如果转换失败，说明 Job 并没有实现 Codec 接口的所有方法。
var _ Codec = (*Job)(nil)

func (c *Job) ReadHeader(h *Header) error {
	return c.decode.Decode(h)
}

func (c *Job) ReadBody(body interface{}) error {
	log.Println("read body")
	defer log.Println("end read body")
	return c.decode.Decode(body)
}

func (c *Job) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = c.buff.Flush()
		if err != nil {
			_ = c.Close()
		}
	}()
	if err = c.encode.Encode(h); err != nil {
		log.Println("rpc codec: gob error encoding header:", err)
		return err
	}
	if err = c.encode.Encode(body); err != nil {
		return err
	}
	return err
}

func (c *Job) Close() error {
	return c.conn.Close()
}

// NewJobCodec 创建一个新的JobCodec实例
func NewJobCodec(conn io.ReadWriteCloser) Codec {
	buffer := bufio.NewWriter(conn)
	return &Job{
		conn:   conn,
		buff:   buffer,
		decode: gob.NewDecoder(conn),
		encode: gob.NewEncoder(buffer),
	}
}
