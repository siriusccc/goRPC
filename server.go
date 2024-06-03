package goRPC

import (
	"encoding/json"
	"goRPC/codec"
	"io"
	"log"
	"net"
)

const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber uint32
	CodecType   codec.Type
}

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   codec.JobType,
}

type Server struct{}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

var invalidRequest = struct {}{}

func (server *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println(err)
			return
		}
		go server.ServeConn(conn)
	}
}


func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() {_ = conn.Close()}()
	var opt Option
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
	server.serveCodec(f(conn))
}

func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}
