package main

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	_ "io"
	"io/ioutil"
	"log"
	"net"
	_ "runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matishsiao/gossdb/ssdb"
)

//Service Client for Proxy
type SrvClient struct {
	Conn        *net.TCPConn
	mu          *sync.Mutex
	RemoteAddr  string
	RequestTime int64
	recvBuf     bytes.Buffer
	tmpBuf      []byte
	DBNodes     []*DBNode
	Auth        bool
	Zip         bool
	TmpResult   []string
	Connected   bool
}

//Client Init
func (cl *SrvClient) Init(conn *net.TCPConn) {
	cl.Conn = conn
	cl.Connected = true
	if CONFIGS.Password == "" {
		cl.Auth = true
	}
	cl.RemoteAddr = strings.Split(cl.Conn.RemoteAddr().String(), ":")[0]
	cl.CheckDBNodes()
	go cl.HealthCheck()
	cl.Read()
}

//Close Client connection
func (cl *SrvClient) Close() {
	cl.mu.Lock()
	cl.Conn.Close()
	cl.Connected = false
	cl.Auth = false
	status.Remove()
	for _, v := range cl.DBNodes {
		v.Client.Close()
	}
	cl.DBNodes = nil
	cl.mu.Unlock()

}

func (cl *SrvClient) HealthCheck() {
	cl.RequestTime = time.Now().Unix()
	for cl.Connected {
		if time.Now().Unix()-cl.RequestTime >= CONFIGS.Timeout {
			log.Println("HealthCheck Service Connection by Timeout:", cl.Conn.RemoteAddr())
			break
		}
		time.Sleep(1 * time.Second)
	}
	cl.Close()
	//receyle client
	cl = nil
}

func (cl *SrvClient) Read() {
	for cl.Connected {
		data, err := cl.Recv()
		if err != nil {
			if CONFIGS.Debug {
				log.Printf("Srv Client Receive Error:%v RemoteAddr:%s\n", err, cl.RemoteAddr)
			}
			break
		} else {
			if len(data) > 0 {
				cl.RequestTime = time.Now().Unix()
				cl.Process(data)
			}
		}
	}
	cl.Close()
}

func (cl *SrvClient) Process(req []string) {
	if len(req) == 0 {
		//ok, not_found, error, fail, client_error
		cl.Send([]string{"error", "request format incorrect."}, false)
	} else {
		switch req[0] {
		case "auth":
			if CONFIGS.Password != "" {
				if len(req) == 2 {
					if req[1] == CONFIGS.Password {
						cl.Auth = true
						cl.Send([]string{"ok", "1"}, false)
					} else {
						cl.Send([]string{"fail", "password incorrect."}, false)
					}
				} else {
					cl.Send([]string{"fail", "request format incorrect"}, false)
				}
			} else {
				cl.Auth = true
				cl.Send([]string{"ok", "1"}, false)
			}
		case "ping":
			cl.Send([]string{"ok", "1"}, false)
		case "batchexec":
			if cl.Auth {
				if GlobalClient.DBPool.InitFlag {
					if len(req) == 2 {
						var cmdlist [][]string
						err := json.Unmarshal([]byte(req[1]), &cmdlist)
						if err != nil {
							cl.Send([]string{"fail", "batchexec need use json format."}, false)
							return
						}
						var resultlist [][]string
						async := false
						if len(cmdlist) > 0 && len(cmdlist[0]) >= 1 && cmdlist[0][0] == "async" {
							async = true
							cl.Send([]string{"ok", "batchexec use async mode."}, false)
						}
						if async {
							batchConn := cl.CreateBatchConnection()
							if batchConn != nil {
								cmdlist = cmdlist[1:]
								//log.Println("async batch sync:", len(cmdlist))
								var batchList [][]interface{}
								for _, v := range cmdlist {
									var newInt []interface{}
									for _, item := range v {
										newInt = append(newInt, item)
									}
									batchList = append(batchList, newInt)
								}
								err := batchConn.BatchSend(batchList)
								if err != nil {
									log.Printf("batchexec async error:%v\n", err)
								}
								//log.Println("async batch sync done")
								batchConn.Close()
							}
						} else {
							for _, v := range cmdlist {
								if v[0] == "async" {
									continue
								}
								res, err := cl.Query(v)
								if err != nil {
									resultlist = append(resultlist, []string{"error", err.Error()})
									continue
								}
								if res == nil {
									resultlist = append(resultlist, []string{"not_found"})
									continue
								}
								resultlist = append(resultlist, res)
							}
							resultjson, err := json.Marshal(resultlist)
							if err != nil {
								cl.Send([]string{"fail", "batchexec send json result failed." + err.Error()}, false)
								return
							}
							res := []string{"ok", string(resultjson)}
							if cl.Zip {
								cl.Send(res, true)
							} else {
								cl.Send(res, false)
							}

						}

					}
				} else {
					cl.Send([]string{"error", "Proxy service initing."}, false)
					return
				}

			} else {
				cl.Send([]string{"fail", "not auth"}, false)
			}
		case "zip":
			if cl.Auth {
				if len(req) == 2 {
					if req[1] == "1" {
						cl.Zip = true
					} else {
						cl.Zip = false
					}
					cl.Send([]string{"ok", req[1]}, false)
				}
			} else {
				cl.Send([]string{"fail", "not auth"}, false)
			}
		default:
			if cl.Auth {
				if GlobalClient.DBPool.InitFlag {
					res, err := cl.Query(req)
					if err != nil {
						cl.Send([]string{"error", err.Error()}, false)
						return
					}
					if CONFIGS.Debug {
						log.Println("Response:", res)
					}
					if res == nil {
						cl.Send([]string{"not_found"}, false)
						return
					} else {
						if cl.Zip {
							cl.Send(res, true)
						} else {
							cl.Send(res, false)
						}
						return
					}
				} else {
					cl.Send([]string{"error", "Proxy service initing."}, false)
					return
				}
			} else {
				cl.Send([]string{"error", "you need login first"}, false)
				return
			}
		}
	}
}

func (cl *SrvClient) CreateBatchConnection() *ssdb.Client {
	for _, v := range CONFIGS.Nodelist {
		if v.Mode == "main" {
			db, err := ssdb.Connect(v.Host, v.Port, v.Password)
			if CONFIGS.Debug {
				log.Println("Connect to ", v.Host, v.Port)
			}
			if err != nil {
				continue
			}
			db.Debug(CONFIGS.Debug)
			return db
		}
	}
	return nil
}

func (cl *SrvClient) CreateConnection(v DBNodeInfo) {
	db, err := ssdb.Connect(v.Host, v.Port, v.Password)
	if CONFIGS.Debug {
		log.Println("Connect to ", v.Host, v.Port)
	}
	if err != nil {
		log.Println("Connect to Fail:", v.Host, v.Port, err)
	}
	db.Debug(CONFIGS.Debug)
	cl.DBNodes = append(cl.DBNodes, &DBNode{Client: db, Id: v.Id, Info: v})
}

func (cl *SrvClient) CheckDBNodes() {
	if len(cl.DBNodes) == 0 {
		for _, v := range CONFIGS.Nodelist {
			if v.Mode == "main" || v.Mode == "queries" {
				cl.CreateConnection(v)
			}
		}
	} else {
		for _, cv := range CONFIGS.Nodelist {
			add := true
			for _, v := range cl.DBNodes {
				if v.Info.Id == cv.Id && v.Info.Host == cv.Host && v.Info.Port == cv.Port {
					add = false
					break
				}
			}
			if add {
				if cv.Mode == "main" || cv.Mode == "queries" {
					cl.CreateConnection(cv)
				}
			}
		}
	}
}

func (cl *SrvClient) Query(args []string) ([]string, error) {
	find := false
	if CONFIGS.Debug {
		log.Println("Query:", args)
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("bad request:request args length incorrect.")
	}
	var mapList map[string]string
	var tmpList []string
	var response []string
	var counter int
	process := false
	mirror := false
	quit := false
	errFlag := false
	sync := false
	syncDel := false
	var errMsg error
	if len(args) > 0 {
		switch args[0] {
		case "hgetall", "hscan", "hrscan", "multi_hget", "scan", "rscan", "multi_get", "zscan", "zrscan":
			mapList = make(map[string]string)
			process = true
		case "hsize", "hkeys", "keys", "rkeys", "hlist", "hrlist", "zkeys":
			process = true
		case "del", "multi_del", "multi_hdel", "hclear", "hdel", "zdel":
			process = true
			mirror = true
		case "exists", "hexists", "zexists":
			process = true
		case "set", "setx", "setnx", "expire", "ttl", "getset", "incr", "getbit", "setbit", "multi_set", "hset", "hincr", "multi_hset", "zset", "zincr", "multi_zset", "qset", "qget", "qpush", "qpush_front", "qpush_back", "qpop":
			mirror = true
		case "mirror":
			sync = true
			if CONFIGS.Debug {
				log.Println("Main Mirror Sync args:", args, cl.RemoteAddr)
			}
		case "mirror_del":
			syncDel = true
			if CONFIGS.Debug {
				log.Println("Main Mirror Sync Del args:", args, cl.RemoteAddr)
			}
		}

		for _, v := range cl.DBNodes {

			db := v.Client
			if CONFIGS.Debug {
				log.Printf("Process:%v Mirror:%v Args:%v Info:%v\n", process, mirror, args, v.Info)
			}
			if sync {
				if v.Info.Mode == "main" {
					if len(args) > 1 {
						val, err := db.Do(args[1:])
						if err != nil {
							errFlag = true
							errMsg = err
							quit = true
						}
						if !errFlag && val[0] == "ok" {
							response = val
							if CONFIGS.Debug {
								log.Printf("Query Mirror Sync args:%v Response:%v Error:%v Server:%s Port:%d RemoteAddr:%s\n", args, val, err, v.Info.Host, v.Info.Port, cl.RemoteAddr)
							}
							break
						} else if len(response) == 0 {
							response = val
						}
					}
				}
			} else if syncDel {
				if len(args) > 1 && v.Info.Mode != "mirror" {
					val, err := db.Do(args[1:])
					if err != nil {
						errFlag = true
						errMsg = err
					}
					log.Printf("Mirror Sync Del Process args:%v Response:%v Error:%v Server:%s Port:%d RemoteAddr:%s\n", args, val, err, v.Info.Host, v.Info.Port, cl.RemoteAddr)

					if !errFlag && val[0] == "ok" {
						response = val
						if CONFIGS.Debug {
							log.Printf("Query Mirror SyncDel args:%v Response:%v Error:%v Server:%s Port:%d RemoteAddr:%s\n", args, val, err, v.Info.Host, v.Info.Port, cl.RemoteAddr)
						}
					} else if len(response) == 0 {
						response = val
					}
				}
			} else if mirror && !process {
				if v.Info.Mode == "main" {
					val, err := db.Do(args)
					if err != nil {
						errFlag = true
						errMsg = err
					}
					if CONFIGS.Debug {
						log.Println("args:", args, " main Response:", val, "error:", err)
					}
					if !errFlag && len(val) > 0 && val[0] == "ok" {
						response = val
						var mirror_args []string
						if len(args) > 2 {
							SendSubInterface(args[1], args)
						}
						mirror_args = append(mirror_args, "mirror")
						mirror_args = append(mirror_args, args...)
						GlobalClient.Append(mirror_args)
						break
					} else {
						response = val
						break
					}
					if CONFIGS.Debug {
						log.Printf("Query Main Need Mirror args:%v Response:%v Error:%v Server:%s Port:%d RemoteAddr:%s\n", args, val, err, v.Info.Host, v.Info.Port, cl.RemoteAddr)
					}
				}
			} else if mirror && process {

				val, err := db.Do(args)
				if err != nil {
					errFlag = true
					errMsg = err
				}
				//log.Printf("Main Mirror Process args:%v Response:%v Error:%v Server:%s Port:%d RemoteAddr:%s\n", args, val, err, v.Info.Host, v.Info.Port, cl.RemoteAddr)

				if !errFlag && val[0] == "ok" {
					response = val
				} else if len(response) == 0 {
					response = val
				}
				if v.Info.Mode == "main" {
					var mirror_args []string
					mirror_args = append(mirror_args, "mirror_del")
					mirror_args = append(mirror_args, args...)
					GlobalClient.Append(mirror_args)
				}
			} else if v.Info.Mode != "mirror" {
				val, err := db.Do(args)
				if err != nil {
					errFlag = true
					errMsg = err
				}
				if CONFIGS.Debug {
					log.Println("args:", args, " Do Response:", val, "error:", err)
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
						size, err := strconv.Atoi(val[1])
						if err != nil {
							log.Println("hsize change fail:", err, val[1])
						}
						counter += size
					case "hkeys", "keys", "rkeys", "hlist", "hrlist":
						val = val[1:]
						if CONFIGS.Debug {
							log.Println("keys val:", val)
						}
						for _, kv := range val {
							kfind := false
							for _, rv := range tmpList {
								if kv == rv {
									kfind = true
									break
								}
							}
							if !kfind {
								tmpList = append(tmpList, kv)
							}
						}
					case "del", "multi_del", "hclear", "hdel", "multi_hdel":
						response = val
					case "exists", "hexists":
						response = val
						if len(val) == 2 && val[1] == "1" {
							quit = true
						}
					default:
						length := len(val[1:])
						if length%2 == 0 {
							data := val[1:]
							for i := 0; i < length; i += 2 {
								if _, ok := mapList[data[i]]; !ok {
									mapList[data[i]] = data[i+1]
								}
							}
						} else {
							log.Println("query failed:", args, "Return:", val)
							response = val
							errMsg = fmt.Errorf("bad request:request length incorrect.")
							quit = true
							errFlag = true
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
		return nil, errMsg
	}

	if find {
		limit := -1
		switch args[0] {
		case "hscan", "hrscan", "scan", "rscan", "hkeys", "keys", "rkeys", "hlist", "hrlist":
			argsLimit, err := strconv.Atoi(args[len(args)-1])
			if err != nil {
				log.Println("limit parser error:", err)
			} else {
				limit = argsLimit
			}
			if CONFIGS.Debug {
				log.Println("argsLimit:", argsLimit, " args len:", args[len(args)-1])
			}
			break
		}
		switch args[0] {
		case "hgetall", "hscan", "hrscan", "multi_hget", "multi_get", "scan", "rscan":
			response = append(response, "ok")
			if len(mapList) > 0 {
				keylist := sortedKeys(mapList)
				if args[0] == "rscan" || args[0] == "hrscan" {
					sort.Sort(sort.Reverse(sort.StringSlice(keylist)))
				}
				if CONFIGS.Debug {
					log.Println("keylist:", keylist, " limit:", limit)
				}

				//if data length > limit ,cut it
				if limit != -1 && len(keylist) >= limit {
					keylist = keylist[:limit]
				}
				for _, v := range keylist {
					response = append(response, v)
					response = append(response, mapList[v])
				}
			}
			break
		case "hsize":
			response = append(response, "ok")
			response = append(response, fmt.Sprintf("%d", counter))
			break
		case "hkeys", "keys", "rkeys", "hlist", "hrlist":
			response = append(response, "ok")
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
			response = append(response, tmpList...)
			break
		}
		mapList = nil
		tmpList = nil
		return response, nil
	}
	mapList = nil
	tmpList = nil
	return response, nil

}

func (cl *SrvClient) Send(args []string, zip bool) {
	var buf bytes.Buffer
	if zip {
		buf.WriteString("3")
		buf.WriteByte('\n')
		buf.WriteString("zip")
		buf.WriteByte('\n')
		var zipbuf bytes.Buffer
		w := gzip.NewWriter(&zipbuf)
		for _, s := range args {
			w.Write([]byte(fmt.Sprintf("%d", len(s))))
			w.Write([]byte("\n"))
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
	_, err := cl.Conn.Write(tmpBuf)
	if err != nil {
		if CONFIGS.Debug {
			log.Printf("Srv Client Send Error:%v RemoteAddr:%s\n", err, cl.RemoteAddr)
		}
		cl.Close()
	}
}

func (cl *SrvClient) Recv() ([]string, error) {
	return cl.recv()
}

func (cl *SrvClient) recv() ([]string, error) {
	//tmp := make([]byte, 102400)
	var tmp [102400]byte
	for {
		resp := cl.parse()
		if resp == nil || len(resp) > 0 {
			//log.Println("SSDB Receive:",resp)
			if len(resp) > 0 && resp[0] == "zip" {
				//log.Println("SSDB Receive Zip\n", resp)
				if len(resp[1]) > 1 {
					zipData, err := base64.StdEncoding.DecodeString(resp[1])
					if err != nil {
						return nil, err
					}
					resp = cl.UnZip(zipData)
				} else {
					return resp, nil
				}

			}
			return resp, nil
		}
		n, err := cl.Conn.Read(tmp[0:])
		if err != nil {
			return nil, err
		}
		cl.recvBuf.Write(tmp[0:n])
	}
	/*tmp := make([]byte, 102400)
	for {
		resp := cl.parse()
		if resp == nil || len(resp) > 0 {
			return resp, nil
		}
		n, err := cl.Conn.Read(tmp)
		if err != nil {
			return nil, err
		}
		cl.recvBuf.Write(tmp[0:n])
	}*/
}

func (cl *SrvClient) parse() []string {
	resp := []string{}
	cl.tmpBuf = cl.recvBuf.Bytes()
	var idx, offset, plen int
	idx = 0
	offset = 0

	for {
		if len(cl.tmpBuf) < offset {
			break
		}
		idx = bytes.IndexByte(cl.tmpBuf[offset:], '\n')
		if idx == -1 {
			break
		}
		p := cl.tmpBuf[offset : offset+idx]
		offset += idx + 1
		//fmt.Printf("> [%s]\n", p);
		plen = len(p)
		if plen == 0 || (plen == 1 && p[0] == '\r') {
			if len(resp) == 0 {
				continue
			} else {
				cl.recvBuf.Next(offset)
				return resp
			}
		}

		size, err := strconv.Atoi(string(p))
		if err != nil || size < 0 {
			return nil
		}
		if offset+size >= cl.recvBuf.Len() {
			break
		}

		v := cl.tmpBuf[offset : offset+size]
		resp = append(resp, string(v))
		offset += size + 1
	}

	//fmt.Printf("buf.size: %d packet not ready...\n", len(buf))
	return []string{}
}

func (cl *SrvClient) UnZip(data []byte) []string {
	var buf bytes.Buffer
	buf.Write(data)
	zipReader, err := gzip.NewReader(&buf)
	if err != nil {
		log.Println("[ERROR] New gzip reader:", err)
	}
	defer zipReader.Close()

	zipData, err := ioutil.ReadAll(zipReader)
	if err != nil {
		fmt.Println("[ERROR] ReadAll:", err)
		return nil
	}
	var resp []string

	if zipData != nil {
		Idx := 0
		offset := 0
		hiIdx := 0
		for {
			Idx = bytes.IndexByte(zipData, '\n')
			if Idx == -1 {
				break
			}
			p := string(zipData[:Idx])
			//fmt.Println("p:[",p,"]\n")
			size, err := strconv.Atoi(string(p))
			if err != nil || size < 0 {
				zipData = zipData[Idx+1:]
				continue
			} else {
				offset = Idx + 1 + size
				hiIdx = size + Idx + 1
				resp = append(resp, string(zipData[Idx+1:hiIdx]))
				//fmt.Printf("data:[%s] size:%d Idx:%d\n",str,size,Idx+1)
				zipData = zipData[offset:]
			}

		}
	}
	return resp
}
