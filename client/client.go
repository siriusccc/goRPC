package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"goRPC"
	"goRPC/codec"
	"io"
	"log"
	"net"
	"sync"
)

type Call struct {
	ServiceMethod string
	Sequence      uint64
	Args          interface{}
	Reply         interface{}
	Error         error
	Done          chan *Call
}

// 当调用结束时，会调用 call.done() 通知调用方，支持异步调用
func (call *Call) done() {
	call.Done <- call
}

// Client pending 存储未处理完的请求，键是编号，值是 Call 实例。
type Client struct {
	cc       codec.Codec
	opt      *goRPC.Option
	sending  sync.Mutex
	header   codec.Header
	mu       sync.Mutex
	Sequence uint64
	pending  map[uint64]*Call
	closing  bool
	shutdown bool
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("client shutdown")

func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.closing && !client.shutdown
}

// 将参数 call 添加到 client.pending 中，并更新 client.seq
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Sequence = client.Sequence
	client.pending[call.Sequence] = call
	client.Sequence++
	return call.Sequence, nil
}

// 从 client.pending 中移除 seq 对应的 call，并返回
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// 服务端或客户端发生错误时调用.
// 将 shutdown 设置为 true，且将错误信息通知所有 pending 状态的 call。
func (client *Client) terminateCall(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

/*
接收到的响应有三种可能：
call 不存在： 可能是请求没有发送完整
call 存在但服务端处理异常：h.Error 非空
call 存在且服务端处理正常：读取body中的reply的值
*/
func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Sequence)
		switch {
		// call不存在
		case call == nil:
			err = client.cc.ReadBody(nil)
		// 服务端处理异常
		case h.Error != "":
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		// 正常，读取reply
		default:
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("read body error" + err.Error())
			}
			call.done()
		}
	}
	client.terminateCall(err)
}

// NewClient 先完成协议交换：发送Option给服务端
// 再根据编解码方式，创建子协程调用receive
func NewClient(conn net.Conn, opt *goRPC.Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("codec type error %s", opt.CodecType)
		log.Println(err)
		return nil, err
	}

	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println(err)
		_ = conn.Close()
		return nil, err
	}
	return NewClientCode(f(conn), opt), nil
}

func NewClientCode(codec codec.Codec, opt *goRPC.Option) *Client {
	Client := &Client{
		opt:      opt,
		cc:       codec,
		pending:  make(map[uint64]*Call),
		Sequence: 1,
	}
	return Client
}
