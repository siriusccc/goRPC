package goRPC

import (
	"encoding/json"
	"errors"
	"goRPC/codec"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
)

const MagicNumber = 0x3bef5c

/*
Option 中的 CodeType 指定了 header 和 body 的编码方式

	服务端首先使用 JSON 解码 Option，
	然后通过 Option 的 CodeType 解码剩余的内容。
*/
type Option struct {
	MagicNumber uint32
	CodecType   codec.Type
}

// DefaultOption 使用默认的Option
var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   codec.JobType,
}

type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

// DefaultServer 默认的 Server 实例
var DefaultServer = NewServer()

func (server *Server) Register(service interface{}) error {
	s := newService(service)
	if _, ok := server.serviceMap.LoadOrStore(s.name, s); ok {
		return errors.New("service already registered" + s.name)
	}
	return nil
}

func Register(service interface{}) error {
	return DefaultServer.Register(service)
}

// 把serviceMethod分割成两部分，serviceName + methodName
// 先在 serviceMap 中找到对应的 service 实例，
// 再从 service 实例的 method 中，找到对应的 methodType
func (server *Server) findServer(serviceMethod string) (ser *service, methodtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("invalid service method: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	sv, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("service not found: " + serviceName)
		return
	}
	ser = sv.(*service)
	methodtype = ser.method[methodName]
	if methodtype == nil {
		err = errors.New("method not found: " + methodName)
	}
	return
}

// Accept 让服务器持续监听一个网络接口（net.Listener）等待新的连接
func (server *Server) Accept(lis net.Listener) {
	// for 循环等待 socket 连接建立, 并开启子协程处理
	log.Println("start server accept")
	defer log.Println("end server closed")
	for {
		log.Println("bf accept new connection")
		conn, err := lis.Accept()
		log.Println("server accept")
		defer log.Println("end server closed")
		if err != nil {
			log.Println(err)
			return
		}
		time.Sleep(time.Second)
		log.Println("serve conn")
		go server.ServeConn(conn)
	}
}

/*
ServeConn 先使用 json.NewDecoder 反序列化得到 Option 实例
再检查 MagicNumber 和 CodeType 的值是否正确，
再根据 CodeType 得到对应的消息编解码器，
最后交给 serverCodec 处理
*/
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	log.Println("start conn")
	defer func(conn io.ReadWriteCloser) {
		err := conn.Close()
		if err != nil {
			log.Println("close conn")
		}
	}(conn)

	var opt Option

	// 读取 JSON 编码的数据，并将其解码到 opt 中
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println(err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Println("invalid magic number")
		return
	}
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Println("invalid codec type")
		return
	}
	// 根据指定的编解码器类型创建一个新的编解码器实例
	code := f(conn)
	server.serveCodec(code)
}

var invalidRequest = struct{}{}

/*
读取请求 readRequest
处理请求 handleRequest
回复请求 sendResponse
*/
func (server *Server) serveCodec(cc codec.Codec) {
	log.Println("start codec server")
	sending := new(sync.Mutex)
	// 确保在关闭编解码器之前所有的请求都已经被完全处理。
	wg := new(sync.WaitGroup)
	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				break
			}
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		// 使用了协程并发执行请求, 但是回复请求的报文必须是逐个发送的
		log.Println("read req success")
		go server.handleRequest(cc, req, sending, wg)
	}
	wg.Wait()
	_ = cc.Close()
}

type request struct {
	h            *codec.Header
	argv, replyv reflect.Value
	mtype        *methodType
	svc          *service
}

// 读取请求头信息, 用 ReadHeader 方法来填充 h
func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	log.Println("start read request header")
	defer log.Println("end read request header")
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		log.Println(err)
		if err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
			log.Println(err)
		}
		return nil, err
	}
	return &h, nil
}

// 读取完整的请求, 包括请求头和请求体
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	log.Println("start read request")
	defer log.Println("end read request")
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	req.svc, req.mtype, err = server.findServer(h.ServiceMethod)
	if err != nil {
		return req, err
	}

	// 创建两个入参实例
	req.argv = req.mtype.newArgv()
	req.argv = req.mtype.newReplyv()

	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}

	// 使用 cc.ReadBody() 将请求报文反序列化为第一个入参 argv
	if err = cc.ReadBody(argvi); err != nil {
		log.Println(err)
		return req, err
	}
	return req, nil
}

func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	log.Println("start send response")
	defer log.Println("end send response")
	sending.Lock()
	defer sending.Unlock()

	if err := cc.Write(h, body); err != nil {
		log.Println("err ====", err)
	}
}

func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	log.Println("start handle request")
	defer log.Println("end handle request")
	defer wg.Done()
	log.Println(req.h, req.argv.Elem())
	err := req.svc.call(req.mtype, req.argv, req.replyv)
	if err != nil {
		req.h.Error = err.Error()
		server.sendResponse(cc, req.h, invalidRequest, sending)
		return
	}
	// 使用 reflect.ValueOf () 函数将 interface {} 转成 reflect.Value 类型
	//req.replyv = reflect.ValueOf(fmt.Sprintf("get resp %d", req.h.Sequence))
	server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}

func Accept(lis net.Listener) {
	log.Println("start accept server")
	defer log.Println("end accept server")
	DefaultServer.Accept(lis)
}
