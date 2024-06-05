package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

type Job struct {
	conn   io.ReadWriteCloser
	buff   *bufio.Writer
	decode *gob.Decoder
	encode *gob.Encoder
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

func NewJobCodec(conn io.ReadWriteCloser) Codec {
	buffer := bufio.NewWriter(conn)
	return &Job{
		conn:   conn,
		buff:   buffer,
		decode: gob.NewDecoder(conn),
		encode: gob.NewEncoder(buffer),
	}
}
