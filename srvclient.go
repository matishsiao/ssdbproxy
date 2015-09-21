package main

import (
	"net"
	"sync"
	"log"
	"strings"
	"strconv"
	"bytes"
	"github.com/matishsiao/gossdb/ssdb"
	"fmt"
	"sort"
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
	if CONFIGS.Password == "" {
		cl.Auth = true
	}
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
	    	if CONFIGS.Debug {
	     		log.Printf("[Read Error]:%v\n",err)	
	     	}
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
	/*log.Printf("Receive:%v\nbytes:%v\n",string(data),data)
	for k,v := range data {
		fmt.Printf("[%d]:%s %v\n",k,string(v),v)
	}*/
	req := cl.Parser(string(data))
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

func (cl *SrvClient) Query(args []string) ([]string,error) {
	find := false
	if CONFIGS.Debug {
		log.Println("Query:",args)
	}
	var mapList map[string]string
	var tmpList []string
	var response []string
	var counter int
	process := false
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
		break
	}	
	quit := false
	for _,v := range CONFIGS.Nodelist {
		if CONFIGS.Debug {
			log.Println("Connect to ",v.Host, v.Port)
		}
	    db, err := ssdb.Connect(v.Host, v.Port,v.Password)
	    if(err != nil){
	    	log.Println("db connection error:",err)
	    	continue
	    }
	    errFlag := false
	   	var errMsg error
	    if len(args) > 0 {
	    	val,err := db.Do(args)
	    	if err != nil {
	    		errFlag = true
	    		errMsg = err
	    	}
	    	if CONFIGS.Debug {
	    		log.Println("args:",args," Do Response:",val,"error:",err)
	    	}	
	    	
	    	db.Close()
	    	
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
							data := val[1:]
							for i := 0; i < length; i += 2 {
								if _,ok := mapList[data[i]]; !ok {
									mapList[data[i]] = data[i+1]
								}
							}
	    			}
	    	}
	   	} else {
	    	errFlag = true
	    	errMsg = fmt.Errorf("bad request:request length incorrect.")
	    }
	   	if quit {
	   		break
	   	}
	    		
	    if errFlag {
	    	db.Close()
	    	return nil,errMsg
	    }
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
	    return response,nil
	}
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
			//fmt.Printf("Data Len:%d LenIdx:%d cut:%d Param:%s tmpdata:%s\n",pklen,lenIdx,cut,param,tmpdata)
		} else {
			break
		}
	}
	if CONFIGS.Debug {
		for k,v := range splitarr {
			fmt.Printf("[%d]:%s\n",k,v)
		}
	}
	return splitarr
}

