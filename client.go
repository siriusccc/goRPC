package goRPC

import (
	"encoding/json"
	"errors"
	"fmt"
	"goRPC/codec"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type Call struct {
	ServiceMethod string
	Sequence      uint64
	Error         error
	Args          interface{}
	Reply         interface{}
	Done          chan *Call
}

// 当调用结束时，会调用 call.done() 通知调用方，支持异步调用
func (call *Call) done() {
	log.Println("Call done")
	defer log.Println("end Call done")
	call.Done <- call
}

// Client pending 存储未处理完的请求，键是编号，值是 Call 实例。
type Client struct {
	cc       codec.Codec
	opt      *Option
	sending  sync.Mutex
	header   codec.Header
	mu       sync.Mutex
	Sequence uint64
	pending  map[uint64]*Call
	closing  bool // 调用结束
	shutdown bool // 服务端或客户端发生错误
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("client shutdown")

func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		log.Println("0000")
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
	log.Println("start add call")
	defer log.Println("end add call")
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
	log.Println("start receive")
	defer log.Println("end receive")
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

// 实现发送请求
func (client *Client) send(call *Call) {
	log.Println("start send")
	defer log.Println("end send")
	client.sending.Lock()
	defer client.sending.Unlock()

	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	client.header.ServiceMethod = call.ServiceMethod
	client.header.Sequence = seq
	client.header.Error = ""

	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go 实现异步调用, 暴露给用户调用，返回一个call实例
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	log.Println("start Go")
	defer log.Println("end Go")
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("client don't have any done channel")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)

	return call
}

// Call 实现同步调用
func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
	log.Println("start call")
	defer log.Println("end call")
	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}

// 解析 option 参数
func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("too many options")
	}

	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

func NewClientCode(codec codec.Codec, opt *Option) *Client {
	client := &Client{
		opt:      opt,
		cc:       codec,
		pending:  make(map[uint64]*Call),
		Sequence: 1,
	}
	log.Println("start client receive")
	defer log.Println("end client receive")
	go client.receive()
	return client
}

// NewClient 先完成协议交换：把 Option 配置信息发送给服务端 conn
// 再根据 Option 中的编解码方式，创建子协程调用 receive
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	log.Println("starting new client")
	defer log.Println("finished new client")

	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("codec type error %s", opt.CodecType)
		log.Println(err)
		return nil, err
	}

	// 使用 JSON 编码器将配置选项 opt 编码，并发送到网络连接 conn
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println(err)
		_ = conn.Close()
		return nil, err
	}
	log.Println("----")
	return NewClientCode(f(conn), opt), nil
}

func Dial(network, addr string, opts ...*Option) (client *Client, err error) {
	log.Println("start dail")
	defer log.Println("end dail")
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	time.Sleep(time.Second)
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	log.Println("create client")
	return NewClient(conn, opt)
}
