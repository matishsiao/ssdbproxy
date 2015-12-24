package main

import (
	"sync"
	"log"
	"github.com/matishsiao/gossdb/ssdb"
	"time"
	"fmt"
)

type ServerClient struct {
	Mutex *sync.Mutex
	DBNodes []*DBNode
	DBPool *ServerConnectionPool
	ArgsChannel chan []string
	Running bool
	Process bool
}

type ServerConnectionPool struct {
	Pool map[string][]ServerConnection
	Counter map[string]int
	ConnectionLimit int
}

func (scp *ServerConnectionPool) Init() {
	scp.CheckMirrorDB()
}

func (scp *ServerConnectionPool) Close() {
	if scp.Pool != nil && len(scp.Pool) > 0 {
		for k,sp := range scp.Pool {// sp = server pool
			log.Printf("[%s]: connection size:%d\n",k,len(sp))
			stop := false
			closeCounter := 0
			spLen := len(sp)
			for _,sc := range sp {
				if !sc.InUse && sc.Client.Connected {
					err := sc.Client.Close()
					if err != nil {
						continue
					}
					closeCounter++
				}
			} 
			if !stop {
				log.Println("Connection busy. wait idle connection and close.")
				for {
					for _,sc := range sp {
						if !sc.InUse && sc.Client.Connected {
							err := sc.Client.Close()
							if err != nil {
								continue
							}
							closeCounter++
						}
					}
					
					if closeCounter == spLen {
						stop = true
						break
					}
				}
			}
			log.Printf("DB[%s]: Close success.Total Connection Closed:%d Connections:%d\n",k,closeCounter,spLen)
		}
	}
}

func (scp *ServerConnectionPool) CheckMirrorDB() {
	if scp.Pool == nil || len(scp.Pool) == 0 {
		scp.Pool = make(map[string][]ServerConnection)
		scp.Counter = make(map[string]int)
		for _,v := range CONFIGS.Nodelist {
			if v.Mode == "mirror" || v.Mode == "sync" {
				name := fmt.Sprintf("%s:%d",v.Host,v.Port)
				for i := 0;i < scp.ConnectionLimit;i++ {
					db, err := ssdb.Connect(v.Host, v.Port,v.Password)
					if CONFIGS.Debug {
						log.Println("Connect to ",v.Host, v.Port)
					}
					if err != nil {
					 	log.Printf("Connect to %s:%d Error:%v\n",v.Host, v.Port,err)
					}
					scp.Pool[name] = append(scp.Pool[name],ServerConnection{Client:db,Info:v,Mu:&sync.Mutex{}})
				}
				log.Printf("Add Mirror Connection[%s][%s]:%d Connections.",name,v.Mode ,len(scp.Pool[name]))
			}
		}
		for k,v := range scp.Pool {
			log.Printf("CheckMirrorDB[%s]: connection size:%d\n",k,len(v))
		}
	}
}

func (scp *ServerConnectionPool) Status() {
	for k,sp := range scp.Pool {
		for _,sc := range sp {
			log.Printf("Run Status[%s] id:%s use:%v connected:%v\n",k,sc.Client.Id,sc.InUse,sc.Client.Connected)
		}
	}
}

func (scp *ServerConnectionPool) CheckStatus() {
	for k,sp := range scp.Pool {
		for _,sc := range sp {
			//log.Printf("Run CheckStatus Start:[%s] id:%s use:%v connected:%v\n",k,sc.Client.Id,sc.InUse,sc.Client.Connected)
			if !sc.InUse {
				if sc.Client.Connected {
					err := sc.Run([]string{"ping"})
					if err != nil {
						log.Printf("CheckStatus[%s][%s]:Run Client Error:%v\n",k,sc.Client.Id,err)
					}
				} else {
					log.Printf("CheckStatus[%s][%s]:Run Client has closed.\n",k,sc.Client.Id)
				}
			}
			log.Printf("CheckStatus:[%s] id:%s use:%v connected:%v\n",k,sc.Client.Id,sc.InUse,sc.Client.Connected)
		}
	}
}

func (scp *ServerConnectionPool) Run(args []string) error {
	var globalErr error
	//log.Printf("Pool Run:%v\n",args)
	for idx,sp := range scp.Pool {// sp = server pool
		//log.Printf("Run[%s]: connection size:%d\n",k,len(sp))
		run := false
		if !run {
			for {
				sc := sp[scp.Counter[idx]]
				scp.Counter[idx]++
				if scp.Counter[idx] >= len(sp) {
						scp.Counter[idx] = 0
				}
				if !sc.InUse {
					err := sc.Run(args)
					if err != nil {
						globalErr = err
					}
					run = true
					//log.Printf("Run[%s][%s][%d]:Run Success\n",idx,sc.Client.Id,scp.Counter[idx])
					break
				}
				//time.Sleep(10 * time.Microsecond)
			}
		}
		//log.Printf("DB[%s]: Run success. Error:%v\n",k,globalErr)
	}
	return globalErr
}
 
type ServerConnection struct {
	Info	DBNodeInfo
	Client *ssdb.Client
	InUse	bool 
	Mu		*sync.Mutex
}

func (sc *ServerConnection) Run(args []string) error {
	var err error
	var run_args []string = args
	
	if sc.Info.Mode == "sync" {
		run_args = run_args[1:]
	}
	db := sc.Client
	sc.Mu.Lock()
	sc.InUse = true
	sc.Mu.Unlock()    
	if db.Connected && !db.Retry {
		_,err = db.Do(run_args)
		if err != nil {
			log.Printf("Mirror Run pool failed on Client[%s] args:%v Error:%v",db.Id,args,err)
		}
	} else {
		err = fmt.Errorf("wait db connection retry.")
	}
	sc.Mu.Lock()
	sc.InUse = false
	sc.Mu.Unlock() 
	return err			
}

func (cl *ServerClient) Append(args []string) {
	if !cl.Running {
		cl.Running = true
	}
	if CONFIGS.Debug {
		log.Println("Server Client Append:",args)
	}
	cl.MirrorQuery(args)
}

func (cl *ServerClient) Close() {
	cl.Running = false
	//wait all mirror command done then close connections.
	for {
		if !cl.Process {
			for _, v := range cl.DBNodes {
				v.Client.Close()
			}
			break
		} 
		time.Sleep(100 * time.Millisecond)
	}
	cl.DBNodes = nil
	cl = nil
}

func (cl *ServerClient) MirrorQuery(args []string) {	
	
	if len(args) > 0 {
		cl.Mutex.Lock()
		cl.Process = true
		cl.Mutex.Unlock()
		err := cl.DBPool.Run(args)
		//if some date save failed,we will retry again.
		if err != nil {
			time.Sleep(1 * time.Second)
			//log.Println("Mirror query failed wait args:",args,"error:",err)	
			go cl.Append(args)
		} 
		cl.Mutex.Lock()
		cl.Process = false
		cl.Mutex.Unlock()
		//log.Printf("MirrorQuery Check Args:%v Error:%v\n",args,err)	
	}	
}