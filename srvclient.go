package main

import (
	"net"
	"sync"
	"log"
	"strings"
	"strconv"
	"bytes"
	"fmt"
)

type SrvClient struct {
	Conn *net.TCPConn
	mu *sync.Mutex
	Auth bool
	Connected bool
}

func (cl *SrvClient) Init(conn *net.TCPConn) {
	cl.Conn = conn
	cl.Connected = true
	//cl.Conn.SetReadDeadline(time.Now().Add(time.Duration(envConfig.Configs.Server.Timeout) * time.Second))
	cl.Read()
}

func (cl *SrvClient) Close() {
	cl.mu.Lock()
	cl.Conn.Close()	
	cl.Connected = false
	cl.Auth = false
	cl.mu.Unlock()
}

func (cl *SrvClient) Write(data []byte) {
	_,err := cl.Conn.Write(data)
	if err != nil {
		cl.Close()
	}
}

func (cl *SrvClient) Read() {
	buf := make([]byte,2048)
	for cl.Connected {
		bytesRead, err := cl.Conn.Read(buf)
	    if err != nil {
	     	log.Printf("[Read Error]:%v\n",err)	
	     	cl.Close()
	     	break
	    } else {
	    	data := buf[:bytesRead]
	    	if len(data) > 0 { 
	    		cl.Process(data)
	    	}
	    }
    }
}

func (cl *SrvClient) Process(data []byte) {
	log.Printf("Receive:%v\nbytes:%v\n",string(data),data)
	for k,v := range data {
		fmt.Printf("[%d]:%s %v\n",k,string(v),v)
	}
	req := cl.Parser(string(data))
	if len(req) == 0 {
		//ok, not_found, error, fail, client_error
		cl.Send([]string{"error","request format incorrect."})
	} else {
		switch req[0] {
			case "auth":
				cl.Send([]string{"ok"})
			break
			case "hget":
				cl.Send([]string{"ok","server"})
			break	
		}
	}
}

func (cl *SrvClient) Send(args []string) {
	var buf bytes.Buffer
	for _, s := range args {
		buf.WriteString(fmt.Sprintf("%d", len(s)))
		buf.WriteByte('\n')
		buf.WriteString(s)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	//_, err := cl.Write(buf.Bytes())
	_,err := cl.Conn.Write(buf.Bytes())
	if err != nil {
		fmt.Println("Client send error:",err)
		cl.Close()
	}
}

func (cl *SrvClient) Parser(data string)[]string {
	tmpdata := data
	var splitarr []string
	for true {
		lenIdx := strings.Index(tmpdata,"\n")
		
		if lenIdx != -1 && lenIdx != 0 {
			pklen,err := strconv.Atoi(tmpdata[:lenIdx])
			if err != nil {
				fmt.Println("conv string error:",err)
			}
			cut := lenIdx+1+pklen
			if cut >= len(tmpdata) {
				cut = len(tmpdata) -1
			}
			param := tmpdata[lenIdx+1:cut]
			
			splitarr = append(splitarr,param)
			tmpdata = tmpdata[cut+1:]
			fmt.Printf("Data Len:%d LenIdx:%d cut:%d Param:%s tmpdata:%s\n",pklen,lenIdx,cut,param,tmpdata)
		} else {
			break
		}
	}
	for k,v := range splitarr {
		fmt.Printf("[%d]:%s\n",k,v)
	}
	return splitarr
}

