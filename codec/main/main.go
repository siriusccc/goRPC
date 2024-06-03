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

func startServer(addr chan string) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening on", l.Addr())
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

	_ = json.NewEncoder(conn).Encode(goRPC.DefaultOption)
	cc := codec.NewJobCodec(conn)
	for i := 0; i < 5; i++ {
		h := &codec.Header{
			ServiceMethod: "Foo.sum",
			Sequence:      uint64(i),
		}
		_ = cc.Write(h, fmt.Sprintf("rpc req %d", h.Sequence))
		_ = cc.ReadHeader(h)
		var reply string
		_ = cc.ReadBody(&reply)
		log.Println(reply)
	}
}
