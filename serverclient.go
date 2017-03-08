package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/matishsiao/gossdb/ssdb"
)

type ServerClient struct {
	Mutex     *sync.Mutex
	DBNodes   []*DBNode
	DBNodeLen int
	DBPool    *ServerConnectionPool
	Running   bool
	Process   bool
	ArgsQueue []*Queue
	QueueIdx  int
}

func (cl *ServerClient) Init() {
	cl.Mutex = &sync.Mutex{}
	cl.Running = true
	cl.DBPool = &ServerConnectionPool{ConnectionLimit: CONFIGS.ConnectionLimit}
	for i := 0; i < cl.DBPool.ConnectionLimit*5; i++ {
		var queue Queue
		queue.Args = make(chan []string)
		cl.ArgsQueue = append(cl.ArgsQueue, &queue)
		go cl.Watcher(queue.Args)
	}
	go cl.DBPool.Init()
	cl.DBNodeLen = 0
	for _, v := range CONFIGS.Nodelist {
		if v.Mode == "mirror" || v.Mode == "sync" {
			cl.DBNodeLen++
		}
	}

}
func (cl *ServerClient) Watcher(queue chan []string) {
	for args := range queue {
		cl.MirrorQuery(args)
		if !cl.Running {
			return
		}
	}
}

func (cl *ServerClient) Append(args []string) {
	if !cl.Running {
		cl.Running = true
	}
	if CONFIGS.Debug {
		log.Println("Server Client Append:", args)
	}
	//no need to sync
	if cl.DBNodeLen == 0 {
		return
	}
	cl.Mutex.Lock()
	cl.QueueIdx++
	if cl.QueueIdx >= len(cl.ArgsQueue) {
		cl.QueueIdx = 0
	}
	idx := cl.QueueIdx
	cl.Mutex.Unlock()
	cl.ArgsQueue[idx].Add(args)
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
	cl.DBPool.Close()
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
			log.Println("Mirror query failed args:", args, "error:", err)
			//go cl.Append(args)
		}
		cl.Mutex.Lock()
		cl.Process = false
		cl.Mutex.Unlock()
		//log.Printf("MirrorQuery Check Args:%v Error:%v\n",args,err)
	}
}

type Queue struct {
	Args chan []string
}

func (queue *Queue) Add(args []string) {
	go queue.add(args)

}

func (queue *Queue) add(args []string) {
	queue.Args <- args
}

type ServerConnectionPool struct {
	Pool            map[string][]ServerConnection
	Counter         map[string]int
	Mutex           map[string]*sync.Mutex
	ConnectionLimit int
	InitFlag        bool
}

func (scp *ServerConnectionPool) Init() {
	scp.CheckMirrorDB()
}

func (scp *ServerConnectionPool) Close() {
	if scp.Pool != nil && len(scp.Pool) > 0 {
		for k, sp := range scp.Pool { // sp = server pool
			log.Printf("[%s]: connection size:%d\n", k, len(sp))
			stop := false
			closeCounter := 0
			spLen := len(sp)
			for _, sc := range sp {
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
					for _, sc := range sp {
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
			log.Printf("DB[%s]: Close success.Total Connection Closed:%d Connections:%d\n", k, closeCounter, spLen)
		}
	}
}

func (scp *ServerConnectionPool) CheckMirrorDB() {
	if scp.Pool == nil || len(scp.Pool) == 0 {
		scp.Pool = make(map[string][]ServerConnection)
		scp.Counter = make(map[string]int)
		scp.Mutex = make(map[string]*sync.Mutex)
		for _, v := range CONFIGS.Nodelist {
			if v.Mode == "mirror" || v.Mode == "sync" {
				name := fmt.Sprintf("%s:%d", v.Host, v.Port)
				scp.Mutex[name] = &sync.Mutex{}
				go scp.ConnectToDB(name, v)
			}
		}

		for k, v := range scp.Pool {
			log.Printf("CheckMirrorDB[%s]: connection size:%d\n", k, len(v))
		}
		time.Sleep(time.Second)
	}
	scp.InitFlag = true
}

func (scp *ServerConnectionPool) ConnectToDB(name string, v DBNodeInfo) {
	for i := 0; i < scp.ConnectionLimit; i++ {
		db, err := ssdb.Connect(v.Host, v.Port, v.Password)
		if CONFIGS.Debug {
			log.Println("Connect to ", v.Host, v.Port)
		}
		if err != nil {
			log.Printf("Connect to %s:%d Error:%v\n", v.Host, v.Port, err)
			continue
		}
		db.Debug(CONFIGS.Debug)
		//default use zip transfered data
		if v.Mode == "mirror" {
			db.UseZip(true)
			db.Do("zip", 1)
		}
		scp.Pool[name] = append(scp.Pool[name], ServerConnection{Client: db, Info: v, Mu: &sync.Mutex{}})
	}
	log.Printf("Add Mirror Connection[%s][%s]:%d Connections.", name, v.Mode, len(scp.Pool[name]))
}

func (scp *ServerConnectionPool) Status() {
	for k, sp := range scp.Pool {
		for _, sc := range sp {
			log.Printf("Run Status[%s] id:%s use:%v connected:%v\n", k, sc.Client.Id, sc.InUse, sc.Client.Connected)
		}
	}
}

func (scp *ServerConnectionPool) CheckStatus() {
	for k, sp := range scp.Pool {
		for _, sc := range sp {
			//log.Printf("Run CheckStatus Start:[%s] id:%s use:%v connected:%v\n",k,sc.Client.Id,sc.InUse,sc.Client.Connected)
			if !sc.InUse {
				if sc.Client.Connected {
					err := sc.Run([]string{"ping"})
					if err != nil {
						log.Printf("CheckStatus[%s][%s]:Run Client Error:%v\n", k, sc.Client.Id, err)
					}
				} else {
					log.Printf("CheckStatus[%s][%s]:Run Client has closed.\n", k, sc.Client.Id)
				}
			}
			log.Printf("CheckStatus:[%s] id:%s use:%v connected:%v\n", k, sc.Client.Id, sc.InUse, sc.Client.Connected)
		}
	}
}

func (scp *ServerConnectionPool) Run(args []string) error {
	var globalErr error
	//log.Printf("Pool Run:%v\n",args)
	for idx, sp := range scp.Pool { // sp = server pool
		//log.Printf("Run[%s]: connection size:%d\n",k,len(sp))
		run := false
		if !run {
			for {
				scp.Mutex[idx].Lock()
				sc := sp[scp.Counter[idx]]
				scp.Counter[idx]++
				if scp.Counter[idx] >= len(sp) {
					scp.Counter[idx] = 0
				}
				scp.Mutex[idx].Unlock()
				if !sc.InUse {
					err := sc.Run(args)
					if err != nil {
						globalErr = err
					}
					run = true
					break
				}
			}
		}
		//log.Printf("DB[%s]: Run success. Error:%v\n",k,globalErr)
	}
	return globalErr
}

type ServerConnection struct {
	Info   DBNodeInfo
	Client *ssdb.Client
	InUse  bool
	Mu     *sync.Mutex
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
		_, err = db.Do(run_args)
		if err != nil {
			log.Printf("Mirror Run pool failed on Client[%s] args:%v Error:%v", db.Id, args, err)
		}
	} else {
		err = fmt.Errorf("wait db connection retry.")
	}
	sc.Mu.Lock()
	sc.InUse = false
	sc.Mu.Unlock()
	return err
}
