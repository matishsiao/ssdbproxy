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
	"compress/gzip"
	"encoding/base64"
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
	Auth bool
	TmpResult []string
	Connected bool
}

func (cl *SrvClient) Init(conn *net.TCPConn) {
	cl.Conn = conn
	cl.Connected = true
	if CONFIGS.Password == "" {
		cl.Auth = true
	}
	cl.RemoteAddr = strings.Split(cl.Conn.RemoteAddr().String(),":")[0]
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
	cl.mu.Unlock()
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
			log.Printf("Srv Client Receive Error:%v RemoteAddr:%s\n",err,cl.RemoteAddr)
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
		cl.Send([]string{"error","request format incorrect."},false)
	} else {
		switch req[0] {
			case "auth":
				if CONFIGS.Password != "" {
					if len(req) == 2 {
						if req[1] == CONFIGS.Password {
							cl.Auth = true
							cl.Send([]string{"ok","1"},false)
						} else {
							cl.Send([]string{"fail","password incorrect."},false)
						}
					} else {
						cl.Send([]string{"fail","request format incorrect"},false)
					}
				} else {
					cl.Auth = true
					cl.Send([]string{"ok","1"},false)
				}	
			break
			default:
				if cl.Auth {
					res,err := cl.Query(req)
					if err != nil {
							cl.Send([]string{"error",err.Error()},false)
						}
					if CONFIGS.Debug {
						log.Println("Response:",res)
					}
					if res == nil {
						cl.Send([]string{"not_found"},false)
					} else {
						//start_time := time.Now().UnixNano()
						if len(res) > CONFIGS.Zip {
							cl.Send(res,true)
						} else {
							cl.Send(res,false)
						}
						/*use_time := (time.Now().UnixNano() - start_time)/1000000
						log.Println("Send use time:",use_time)*/
					}
				} else {
					cl.Send([]string{"error","you need login first"},false)
				}
		}
	}
}
func (cl *SrvClient) CheckDBNodes() {
	if len(cl.DBNodes) == 0 {
		for _,v := range CONFIGS.Nodelist {
			if v.Mode != "mirror" {
				db, err := ssdb.Connect(v.Host, v.Port,v.Password)
				if CONFIGS.Debug {
					log.Println("Connect to ",v.Host, v.Port)
				}
				if err != nil {
				 	continue
				}
				cl.DBNodes = append(cl.DBNodes,&DBNode{Client:db,Id:v.Id,Info:v})
			}
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
	//var mapList map[string]string
	var resultList []SrvData
	var tmpList []string
	var response []string
	var counter int
	process := false
	mirror := false
	quit := false
	errFlag := false
	sync := false
	syncDel := false
	//var start_time int64
	var errMsg error
	if len(args) > 0 {
		switch args[0] {
			case "hgetall","hscan","hrscan","multi_hget","scan","rscan","multi_get","zscan","zrscan":
				//mapList = make(map[string]string)
				process = true
			break
			case "hsize","hkeys","keys","rkeys","hlist","hrlist","zkeys":
				process = true
			break	
			case "del","multi_del","multi_hdel","hclear","hdel","zdel":
				process = true
				mirror = true
			break
			case "exists","hexists","zexists":
				process = true
			break
			case "set","setx","setnx","expire","ttl","getset","incr","getbit","setbit","multi_set","hset","hincr","multi_hset","zset","zincr","multi_zset","qset","qpush","qpush_front","qpush_back","qpop":
				mirror = true
			break
			case "mirror":
				sync = true
				if CONFIGS.Sync { 
				    log.Println("Main Mirror Sync args:",args,cl.RemoteAddr)
			  	}
			break
			case "mirror_del":
				syncDel = true
				if CONFIGS.Sync { 
				    log.Println("Main Mirror Sync Del args:",args,cl.RemoteAddr)
			  	}
			break
			
		}
		cl.CheckDBNodes()
		for _,v := range cl.DBNodes {
			
			db := v.Client
			if CONFIGS.Debug {
				log.Printf("Process:%v Mirror:%v Args:%v Info:%v\n",process,mirror,args,v.Info)
		    }
			if sync {
				if v.Info.Mode == "main" {
					if len(args) > 1 {
						val,err := db.Do(args[1:])
				    	if err != nil {
				    		errFlag = true
					   		errMsg = err
					   		quit = true
					   	}
				    	if !errFlag && val[0] == "ok" {
				    		response = val
				    		if CONFIGS.Sync { 
					    		log.Printf("Query Mirror Sync args:%v Response:%v Error:%v Server:%s Port:%d RemoteAddr:%s\n",args,val,err,v.Info.Host,v.Info.Port,cl.RemoteAddr)
							}
				    		break
				    	} else if len(response) == 0 {
				    		response = val
				    	}	
			    	}
				}
			} else if syncDel {
				if len(args) > 1 && v.Info.Mode != "mirror" {
					val,err := db.Do(args[1:])
				    if err != nil {
				    	errFlag = true
					   	errMsg = err
					}
				    log.Printf("Mirror Sync Del Process args:%v Response:%v Error:%v Server:%s Port:%d RemoteAddr:%s\n",args,val,err,v.Info.Host,v.Info.Port,cl.RemoteAddr)
				
				   	if !errFlag && val[0] == "ok" {
				   		response = val
				   		if CONFIGS.Sync { 
				    		log.Printf("Query Mirror SyncDel args:%v Response:%v Error:%v Server:%s Port:%d RemoteAddr:%s\n",args,val,err,v.Info.Host,v.Info.Port,cl.RemoteAddr)
						}
				   	} else if len(response) == 0 {
				   		response = val
				   	}	
			   	}
			} else if mirror && !process {
		    	if v.Info.Mode != "queries" {
		    		if v.Info.Mode == "main" {
				   		val,err := db.Do(args)
				    	if err != nil {
				    		errFlag = true
					   		errMsg = err
					   	}
				    	if !errFlag && val[0] == "ok" {
				    		response = val
				    	} else if len(response) == 0 {
				    		response = val
				    	}	
				    	if CONFIGS.Sync { 
				    		log.Printf("Query Main Need Mirror args:%v Response:%v Error:%v Server:%s Port:%d RemoteAddr:%s\n",args,val,err,v.Info.Host,v.Info.Port,cl.RemoteAddr)
						}
				    	var mirror_args []string
			    		mirror_args = append(mirror_args,"mirror")
						mirror_args = append(mirror_args,args...)
						dbClient.Append(mirror_args)
				    }	
		    	}
	   		} else if mirror && process {
	   			
	   			val,err := db.Do(args)
				if err != nil {
					errFlag = true
				  	errMsg = err
				}
				log.Printf("Main Mirror Process args:%v Response:%v Error:%v Server:%s Port:%d RemoteAddr:%s\n",args,val,err,v.Info.Host,v.Info.Port,cl.RemoteAddr)
					
				if !errFlag && val[0] == "ok" {
					response = val
				} else if len(response) == 0 {
					response = val
				}	
	   			if v.Info.Mode == "main" {
	   				var mirror_args []string
			   		mirror_args = append(mirror_args,"mirror_del")
					mirror_args = append(mirror_args,args...)
					dbClient.Append(mirror_args)
	   			}
	   		} else if v.Info.Mode != "mirror" {
	   			//start_time = time.Now().UnixNano()
	   			val,err := db.Do(args)
		    	if err != nil {
		    		errFlag = true
			   		errMsg = err
			   	}
		    	/*if start_time != 0 {
					use_time := (time.Now().UnixNano() - start_time)/1000000
					log.Println("Query use time:",use_time)
				}*/
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
    					case "del","multi_del","hclear","hdel","multi_hdel":
	  						response = val
		    			case "exists","hexists":
		    				response = val
		    				if val[1] == "1" {
		    					quit = true
		    				}
		    			default:
			    			length := len(val[1:])
							if length % 2 == 0 {
								data := val[1:]
								for i := 0; i < length; i += 2 {
									resultList = append(resultList,SrvData{Key:data[i],Value:data[i+1]})
									/*if _,ok := mapList[data[i]]; !ok {
										mapList[data[i]] = data[i+1]
									}*/
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
				if len(resultList) > 0 {
					var keylist []SrvData
					if args[0] == "hrscan" {
						keylist = sortedSrvRKeys(resultList)
					} else {
						keylist = sortedSrvKeys(resultList)
					}
					/*if args[0] == "rscan" || args[0] == "hrscan" {
						sort.Sort(sort.Reverse(sort.StringSlice(keylist)))
					}*/
					if CONFIGS.Debug {
						log.Println("keylist:",keylist, " limit:",limit)
					}
					
					//if data length > limit ,cut it
					if limit != -1 && len(keylist) >= limit {
						keylist = keylist[:limit]
					}
					for _,v := range keylist {
						response = append(response,v.Key)
						response = append(response,v.Value)
					}
				}
				/*if len(mapList) > 0 {
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
			    }*/
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
		//mapList  = nil
		tmpList = nil
		resultList = nil
	    return response,nil
	}
	//mapList  = nil
	tmpList = nil
	resultList = nil
	return response,nil
	
}

func (cl *SrvClient) Send(args []string,zip bool) {
	var buf bytes.Buffer
	if zip {
		buf.WriteString("3")
		buf.WriteByte('\n')
		buf.WriteString("zip")
		buf.WriteByte('\n')
		var zipbuf bytes.Buffer
		w := gzip.NewWriter(&zipbuf)
		for _, s := range args {
			w.Write([]byte(s))
			w.Write([]byte("\n"))
		}
		w.Close()
		zipbuff := base64.StdEncoding.EncodeToString(zipbuf.Bytes())
		buf.WriteString(fmt.Sprintf("%d", len(zipbuff)))
		buf.WriteByte('\n')
		buf.WriteString(zipbuff)
		buf.WriteByte('\n')
		buf.WriteByte('\n')
	} else {
		for _, s := range args {
			buf.WriteString(fmt.Sprintf("%d", len(s)))
			buf.WriteByte('\n')
			buf.WriteString(s)
			buf.WriteByte('\n')
		}
		buf.WriteByte('\n')
	}
	tmpBuf := buf.Bytes()
	_,err := cl.Conn.Write(tmpBuf)
	if err != nil {
		log.Printf("Srv Client Send Error:%v RemoteAddr:%s\n",err,cl.RemoteAddr)
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