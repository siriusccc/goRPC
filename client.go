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
	Args          interface{} // 传递给方法的参数
	Reply         interface{} // 存储远程方法返回的结果
	Done          chan *Call  // 回调函数，在RPC调用完成时通知调用者
}

// 当调用结束时，会调用 call.done() 通知调用方，支持异步调用
// 将 Call 实例发送到 Done 通道，以通知调用者可以检查调用的结果
func (call *Call) done() {
	log.Println("Call done")
	defer log.Println("end Call done")
	call.Done <- call
}

// Client 结构体，代表一个RPC客户端
type Client struct {
	cc       codec.Codec      // 用于编码和解码
	opt      *Option          // 客户端配置
	sending  sync.Mutex       // 互斥锁，用于保护写操作，确保同一时间只有一个goroutine可以发送请求
	header   codec.Header     // 存储RPC请求的头部信息
	mu       sync.Mutex       // 另一个互斥锁，用于保护客户端的状态字段
	Sequence uint64           // 序列号，用于唯一标识每个RPC请求。
	pending  map[uint64]*Call // pending 存储未处理完的请求, 键是编号, 值是 Call 实例
	closing  bool             // 表示用户是否主动关闭了客户端
	shutdown bool             // 服务端或客户端发生错误。服务器是否通知客户端关闭
}

// 确保 Client 实现了 io.Closer 接口
var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("client shutdown")

// Close 用于关闭客户端，通过设置 closing 字段为 true，并关闭编解码器
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

// IsAvailable 检查客户端是否可用于发送新的RPC请求
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.closing && !client.shutdown
}

// 将一个新的RPC调用注册到客户端。 将参数 call 添加到 client.pending 中，并更新 client.seq
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

// 从客户端移除一个指定序列号的RPC调用。 从 client.pending 中移除 seq 对应的 call，并返回
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// 终止所有挂起的RPC调用，客户端关闭或遇到无法恢复的错误时调用
// 将 shutdown 设置为 true，遍历所有挂起的调用，且将错误信息通知所有 pending 状态的 call。
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

// 发送一个RPC调用请求到服务端
// 注册调用到pending -> 请求头部 -> 发送请求
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

// Go 实现异步调用RPC服务
// 调用者可以通过 done 通道接收回调通知，了解调用是否完成
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

// Call 是 Go 方法的同步版本
// 等待 done 通道接收到完成通知，然后返回调用的错误状态
func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
	log.Println("start call")
	defer log.Println("end call")

	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}

// 解析客户端配置选项
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

// NewClientCode 实际构造 Client 实例的地方
// 启动 receive Goroutine，负责接收来自服务端的响应。
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

// NewClient 创建一个新的 RPC Client 实例。
// 先完成协议交换：先根据编解码器类型创建编解码器，再把配置信息发送给服务端 conn，
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
	return NewClientCode(f(conn), opt), nil
}

// Dial 连接RPC服务器
// 解析选项 -> 建立网络连接 -> 创建客户端实例
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
