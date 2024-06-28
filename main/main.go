package main

import (
	"goRPC"
	"log"
	"net"
	"sync"
	"time"
)

// 启动RPC服务器并监听TCP连接, 随机分配的端口。 确保服务端端口监听成功, 客户端再发起请求
func startServer(addr chan string) {
	var foo Foo
	if err := goRPC.Register(&foo); err != nil {
		log.Fatal(err)
	}
	log.Println("Starting server")
	defer log.Println("Server stopped")
	// 监听 TCP 连接, 0 为自动分配一个可用的端口
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening on", l.Addr())
	addr <- l.Addr().String()
	goRPC.Accept(l)
}

type Foo int

type Args struct{ Num1, Num2 int }

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

/*
客户端先发送 Option, 进行协议交换
再发送 消息头header 和 消息体,
最后解析服务端响应 reply
*/
func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	// 向外发送服务器监听的地址
	addr := make(chan string)
	go startServer(addr)

	// 创建一个客户端连接到服务器
	client, _ := goRPC.Dial("tcp", <-addr)
	defer func() {
		_ = client.Close()
	}()

	time.Sleep(time.Second)
	// 将 goRPC.DefaultOption 编码为 JSON 并写入到 conn。编码后的 JSON 数据将直接写入到 conn
	// 将客户端的默认配置序列化为 JSON 格式，并通过建立的 TCP 连接发送给服务器
	var wg sync.WaitGroup
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// RPC调用的参数
			args := &Args{
				i,
				i * i,
			}
			// 接收RPC调用的结果
			var reply string
			// 发起RPC请求，方法名是"Foo.Sum"，参数是args，结果填充在reply变量中。
			err := client.Call("Foo.Sum", args, &reply)
			log.Println(reply, err)
			if err != nil {
				log.Fatal("call error:", err)
			}
			log.Printf("%d + %d = %d", args.Num1, args.Num2, reply)
		}(i)
	}
	wg.Wait()
}
