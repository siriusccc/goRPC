package main

import (
	"encoding/json"
	"fmt"
	"goRPC"
	"goRPC/codec"
	"log"
	"net"
	"time"
)

// 确保服务端端口监听成功，客户端再发起请求
func startServer(addr chan string) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening on", l.Addr())
	fmt.Println("111111")
	addr <- l.Addr().String()
	goRPC.Accept(l)
}

/*
客户端先发送 Option, 进行协议交换
再发送 消息头header 和 消息体,
最后解析服务端响应 reply
*/
func main() {
	addr := make(chan string)
	go startServer(addr)

	conn, _ := net.Dial("tcp", <-addr)
	defer func() {
		_ = conn.Close()
	}()

	time.Sleep(time.Second)
	// 将 goRPC.DefaultOption 编码为 JSON 并写入到 conn。编码后的 JSON 数据将直接写入到 conn
	// 将客户端的默认配置序列化为 JSON 格式，并通过建立的 TCP 连接发送给服务器
	_ = json.NewEncoder(conn).Encode(goRPC.DefaultOption)
	cc := codec.NewJobCodec(conn)
	for i := 0; i < 5; i++ {
		h := &codec.Header{
			ServiceMethod: "Foo.sum",
			Sequence:      uint64(i),
		}
		// 将请求头和请求体写入到连接 conn
		_ = cc.Write(h, fmt.Sprintf("rpc req %d", h.Sequence))
		// 读取响应头，此时 h 被更新为响应的头部信息
		_ = cc.ReadHeader(h)

		// 读取响应体
		var reply string
		_ = cc.ReadBody(&reply)
		log.Println(reply)
	}
}
