package main

import(
	"fmt"
	"net"
	"log"
	"os"
	"sync"
)

var ProxyConn int64

func Listen(ip string,port int) {
	log.Printf("[Server] %v:%v start listen.\n",ip,port)
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d",ip,port))
	if err != nil {
		log.Printf("Listen Error:%v\n",err)
		os.Exit(1)
		return
	}
	ln := l.(*net.TCPListener)
		
	for {
    	conn, err := ln.AcceptTCP()
    	if err != nil {
    		log.Println("Accept Error:",err)
    	} else {
    		ProxyConn++
    		go ProcessConn(conn)
    	}
	}
}

func ProcessConn(c *net.TCPConn) {
	var client SrvClient = SrvClient{mu:&sync.Mutex{}}
	client.Init(c)
}