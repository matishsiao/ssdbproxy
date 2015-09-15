package main

import(
	"fmt"
	"net"
	"log"
	"os"
	"sync"
	_ "io"
	_ "compress/gzip"
)

var srvClientList []*SrvClient
var serverIP string

func Listen(ip string,port int) {
	 
	serverIP = ip
	log.Printf("[Server] %v%v start listen.\n",serverIP,port)
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d",serverIP,port))
	if err != nil {
		log.Printf("Listen Error:%v\n",err)
		os.Exit(1)
		return
	}
	ln := l.(*net.TCPListener)
		
	for {
    	conn, err := ln.AcceptTCP()
    	if err != nil {
    		log.Println(err)
    	} else {
    		go ProcessConn(conn)
    	}
	}
}

func ProcessConn(c *net.TCPConn) {
	client := new(SrvClient)
	client.mu = &sync.Mutex{}
	srvClientList = append(srvClientList,client)
	client.Init(c)
	
}