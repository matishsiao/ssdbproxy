package main

import (
	"net"
	"sync"
	"log"
	_"io"
	"strings"
	"strconv"
	"bytes"
	"github.com/matishsiao/gossdb/ssdb"
	"fmt"
	"sort"
	_"runtime"
	"time"
)

type SrvClient struct {
	Conn *net.TCPConn
	mu *sync.Mutex
	RemoteAddr string
	RequestTime int64
	recv_buf bytes.Buffer
	DBNodes []*DBNode
	dbClient ServerClient
	Auth bool
	Connected bool
}

func (cl *SrvClient) Init(conn *net.TCPConn) {
	cl.Conn = conn
	cl.Connected = true
	if CONFIGS.Password == "" {
		cl.Auth = true
	}
	cl.RemoteAddr = strings.Split(cl.Conn.RemoteAddr().String(),":")[0]
	/*cl.dbClient = ServerClient{Mutex:&sync.Mutex{},Running:true}
	go cl.dbClient.Run()*/
	go cl.HealthCheck()
	cl.Read()
	
}

func (cl *SrvClient) CheckServer(requestClient string) bool {
	for _,cv := range CONFIGS.Nodelist {
		if requestClient == cv.Host && cv.Mode == "mirror" {
			return true
		}
	}
	return false
}

func (cl *SrvClient) Close() {
	cl.mu.Lock()
	cl.Conn.Close()	
	cl.Connected = false
	cl.Auth = false
	for _,v := range cl.DBNodes {
		v.Client.Close()
	}
	if CONFIGS.Debug {
		log.Println("Close Service Connection by Timeout:",cl.Conn.RemoteAddr(),"Close DB Connections:",len(cl.DBNodes))
	}	
	cl.DBNodes = nil
	cl.dbClient.Close()
	cl.mu.Unlock()
}

func (cl *SrvClient) Write(data []byte) {
	_,err := cl.Conn.Write(data)
	if err != nil {
		cl.Close()
	}
}

func (cl *SrvClient) HealthCheck() {
	cl.RequestTime = time.Now().Unix()
	timeout := 1
	for cl.Connected {
		if time.Now().Unix() - cl.RequestTime >= CONFIGS.Timeout {
			cl.Close()
			break
		}
		time.Sleep(time.Duration(timeout) * time.Second)
    }
	//receyle client
	cl = nil
}

func (cl *SrvClient) Read() {
	timeout := 100
	
	for cl.Connected {
		data, err := cl.Recv()
		if err != nil {
			//log.Println("Read Error:",err,cl.Conn.RemoteAddr())
			//timeout = 200
			cl.Close()
		} else {
			cl.RequestTime = time.Now().Unix()
			if len(data) > 0 {
				go cl.Process(data)
			}
			timeout = 10
		}
		time.Sleep(time.Duration(timeout) * time.Microsecond)
    }
}

func (cl *SrvClient) Process(req []string) {
	if len(req) == 0 {
		//ok, not_found, error, fail, client_error
		cl.Send([]string{"error","request format incorrect."})
	} else {
		switch req[0] {
			case "auth":
				if CONFIGS.Password != "" {
					if len(req) == 2 {
						if req[1] == CONFIGS.Password {
							cl.Auth = true
							cl.Send([]string{"ok","1"})
						} else {
							cl.Send([]string{"fail","password incorrect."})
						}
					} else {
						cl.Send([]string{"fail","request format incorrect"})
					}
				} else {
					cl.Auth = true
					cl.Send([]string{"ok","1"})
				}	
			break
			default:
				if cl.Auth {
					res,err := cl.Query(req)
					if err != nil {
						cl.Send([]string{"error",err.Error()})
					}
					if CONFIGS.Debug {
						log.Println("Response:",res)
					}
					if res == nil {
						cl.Send([]string{"not_found"})
					} else {
						cl.Send(res)
					}
				} else {
					cl.Send([]string{"error","you need login first"})
				}
		}
	}
}
func (cl *SrvClient) CheckDBNodes() {
	if len(cl.DBNodes) == 0 {
		for _,v := range CONFIGS.Nodelist {
			db, err := ssdb.Connect(v.Host, v.Port,v.Password)
			if CONFIGS.Debug {
				log.Println("Connect to ",v.Host, v.Port)
			}
			if err != nil {
			 	continue
			}
			cl.DBNodes = append(cl.DBNodes,&DBNode{Client:db,Id:v.Id,Info:v})
		}
	} else {
		for _,cv := range CONFIGS.Nodelist {
			add := true
			for _,v := range cl.DBNodes {
				if v.Info.Id == cv.Id && v.Info.Host == cv.Host && v.Info.Port == cv.Port {
					add = false
					break
				}
			}
			if add {
				db, err := ssdb.Connect(cv.Host, cv.Port,cv.Password)
				if CONFIGS.Debug {
					log.Println("Connect to ",cv.Host, cv.Port)
				}
				if err != nil {
				 	continue
				}
				cl.DBNodes = append(cl.DBNodes,&DBNode{Client:db,Id:cv.Id,Info:cv})
			}
		}
	}
}

func (cl *SrvClient) Query(args []string) ([]string,error) {
	find := false
	if CONFIGS.Debug {
		log.Println("Query:",args)
	}
	if len(args) == 0 {
		return nil,fmt.Errorf("bad request:request args length incorrect.")
	}
	var mapList map[string]string
	var tmpList []string
	var response []string
	var counter int
	process := false
	mirror := false
	quit := false
	errFlag := false
	var errMsg error
	if len(args) > 0 {
		switch args[0] {
			case "hgetall","hscan","hrscan","multi_hget","scan","rscan","multi_get":
				mapList = make(map[string]string)
				process = true
			break
			case "hsize","hkeys","keys","rkeys","hlist","hrlist":
				process = true
			break	
			case "del","multi_del","multi_hdel","exists","hexists","hclear","hdel":
				process = true
				mirror = true
			break
			case "hset","set","zset","hincr","incr","zincr","qset","qincr","setx","getset","setnx":
				mirror = true
			break
		}
		cl.CheckDBNodes()
		for _,v := range cl.DBNodes {
			db := v.Client
			if CONFIGS.Debug {
				log.Printf("Process:%v Mirror:%v Args:%v Info:%v\n",process,mirror,args,v.Info)
		    }
		    if mirror && !process {
		    	if v.Info.Mode != "queries" {
		    		
			   		val,err := db.Do(args)
			    	if err != nil {
			    		errFlag = true
				   		errMsg = err
				   		continue
				   	}
			    	if CONFIGS.Sync && args[1] == "Test" { 
			    		log.Println("Query Mirror args:",args," Do Response:",val,err,v.Info.Host,cl.RemoteAddr)
			    	}
				   	if val[0] == "ok" {
			    		response = val
			    		if cl.CheckServer(cl.RemoteAddr) {
			    			if CONFIGS.Sync && args[1] == "Test" { 
					    		log.Println("Query Mirror args:",args," Do Response:",val,err,v.Info.Host,cl.RemoteAddr)
					    	}
			    			break
			    		}
			    	} else if len(response) == 0 {
			    		response = val
			    	}	
		    	}
	   		} else if mirror && process {
	   			val,err := db.Do(args)
			    if err != nil {
			    	errFlag = true
				   	errMsg = err
				   	continue
				}
			    	
			  	if val[0] == "ok" {
			    	response = val
			    	if CONFIGS.Sync && args[1] == "Test" { 
			    		log.Println("Query Mirror with Process args:",args," Do Response:",v.Info.Host,cl.RemoteAddr)
			    	}
			   	} else if len(response) == 0 {
			   		response = val
			   	}
	   		} else if v.Info.Mode != "mirror" {
	   			val,err := db.Do(args)
		    	if err != nil {
		    		errFlag = true
			   		errMsg = err
			   	}
			   	if CONFIGS.Debug {
			   		log.Println("args:",args," Do Response:",val,"error:",err)
			   	}  	
			   	if !errFlag && !process && len(val) >= 1 {
			   		if val[0] == "ok" {
			   			response = val
			   			break
			   		} else if len(response) == 0 {
			   			response = val
			   		}	
			   	}		
		    	if !errFlag && len(val) >= 1 && val[0] != "not_found" {
		    		find = true
		    		switch args[0] {
	    				case "hsize":
		   					size,err := strconv.Atoi(val[1])
		   					if err != nil {
		   						log.Println("hsize change fail:",err,val[1])
		   					}
		    				counter += size
		    			break
		    			case "hkeys","keys","rkeys","hlist","hrlist":
		    				val = val[1:]
		    				if CONFIGS.Debug {
								log.Println("keys val:",val)
							}
		    			for _,kv := range val {
		    				kfind := false
		   					for _,rv := range tmpList {
		    					if kv == rv {
		    						kfind = true
									break
		    					}
							}
		    				if !kfind {
		    					tmpList = append(tmpList,kv)
	  						}
	   					}
	   				break
    				case "del","multi_del","hclear","hdel","multi_hdel":
	  					response = val
		   			break
		    		case "exists","hexists":
		    			response = val
		    			if val[1] == "1" {
		    				quit = true
		    			} 
		   			break
		    		default:
		    			length := len(val[1:])
						if length % 2 == 0 {
							data := val[1:]
							for i := 0; i < length; i += 2 {
								if _,ok := mapList[data[i]]; !ok {
									mapList[data[i]] = data[i+1]
								}
							}
						} else {
							log.Println("query failed:",args, "Return:",val)
						}
		    		}
				}
			}
	    	if quit {
	    		break
	    	}
	   	}
    } else {
	  	errFlag = true
	   	errMsg = fmt.Errorf("bad request:request length incorrect.")
	}
			
	if errFlag {
	 	return nil,errMsg
	}
	
	if find {
		limit := -1
		switch args[0] {
			case "hscan","hrscan","scan","rscan","hkeys","keys","rkeys","hlist","hrlist":
				argsLimit,err := strconv.Atoi(args[len(args)-1])
				if err != nil {
					log.Println("limit parser error:",err)
				} else {
					limit = argsLimit
				}
				if CONFIGS.Debug {
					log.Println("argsLimit:",argsLimit, " args len:",args[len(args)-1])
				}
			break
		}
		switch args[0] {
			case "hgetall","hscan","hrscan","multi_hget","multi_get","scan","rscan":
				response = append(response,"ok")
				if len(mapList) > 0 {
					keylist := sortedKeys(mapList)
					if args[0] == "rscan" || args[0] == "hrscan" {
						sort.Sort(sort.Reverse(sort.StringSlice(keylist)))
					}
					if CONFIGS.Debug {
						log.Println("keylist:",keylist, " limit:",limit)
					}
					
					//if data length > limit ,cut it
					if limit != -1 && len(keylist) >= limit {
						keylist = keylist[:limit]
					}
					for _,v := range keylist {
						response = append(response,v)
						response = append(response,mapList[v])
					}
			    }
			break	
			case "hsize":
				response = append(response,"ok")
				response = append(response,fmt.Sprintf("%d",counter))
			break
			case "hkeys","keys","rkeys","hlist","hrlist":
				if CONFIGS.Debug {
					log.Println("tmpList:",tmpList)
				}
				response = append(response,"ok")
				sort.Strings(tmpList)
				if args[0] == "rkeys" || args[0] == "hrlist" {
					sort.Sort(sort.Reverse(sort.StringSlice(tmpList)))
				}
				if limit != -1 {
					if len(tmpList) < limit {
						limit = len(tmpList)
					}
					tmpList = tmpList[:limit]
				}
				response = append(response,tmpList...)
			break
		}
		mapList  = nil
		tmpList = nil
	    return response,nil
	}
	mapList  = nil
	tmpList = nil
	return response,nil
	
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
	_,err := cl.Conn.Write(buf.Bytes())
	if err != nil {
		log.Println("Client send error:",err)
		cl.Close()
	}
}

func (cl *SrvClient) Recv() ([]string, error) {
	return cl.recv()
}

func (cl *SrvClient) recv() ([]string, error) {
	var tmp [10240]byte
	for {
		resp := cl.parse()
		if resp == nil || len(resp) > 0 {
			return resp, nil
		}
		n, err := cl.Conn.Read(tmp[0:])
		if err != nil {
			return nil, err
		}
		cl.recv_buf.Write(tmp[0:n])
	}
}

func (cl *SrvClient) parse() []string {
	resp := []string{}
	buf := cl.recv_buf.Bytes()
	var idx, offset int
	idx = 0
	offset = 0

	for {
		idx = bytes.IndexByte(buf[offset:], '\n')
		if idx == -1 {
			break
		}
		p := buf[offset : offset+idx]
		offset += idx + 1
		//fmt.Printf("> [%s]\n", p);
		if len(p) == 0 || (len(p) == 1 && p[0] == '\r') {
			if len(resp) == 0 {
				continue
			} else {
				cl.recv_buf.Next(offset)
				return resp
			}
		}

		size, err := strconv.Atoi(string(p))
		if err != nil || size < 0 {
			return nil
		}
		if offset+size >= cl.recv_buf.Len() {
			break
		}

		v := buf[offset : offset+size]
		resp = append(resp, string(v))
		offset += size + 1
	}

	//fmt.Printf("buf.size: %d packet not ready...\n", len(buf))
	return []string{}
}