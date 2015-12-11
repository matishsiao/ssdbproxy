package main

import (
	"sync"
	"log"
	"github.com/matishsiao/gossdb/ssdb"
	"time"
)

type ServerClient struct {
	Mutex *sync.Mutex
	ArgsList []ServerArgs
	DBNodes []*DBNode
	Running bool
}

type ServerArgs struct {
	Args []string
	Server string
	Client string
}

func (cl *ServerClient) Append(args []string,runningServer string,requestClient string) {
	server := false
	for _,cv := range CONFIGS.Nodelist {
		if requestClient == cv.Host && cv.Mode == "mirror" {
			server = true
			break
		}
	}
	if !server {
		cl.Mutex.Lock()
		cl.ArgsList = append(cl.ArgsList,ServerArgs{Args:args,Server:runningServer,Client:requestClient})
		cl.Mutex.Unlock()
	}
}

func (cl *ServerClient) Get() *ServerArgs {
	var obj *ServerArgs
	if len(cl.ArgsList) > 0 {
		cl.Mutex.Lock()	
		obj = &cl.ArgsList[0]
		if len(cl.ArgsList) > 1 {
			cl.ArgsList = cl.ArgsList[1:]
		} else {
			cl.ArgsList = nil
		}
		cl.Mutex.Unlock()
	}
	return obj
}

func (cl *ServerClient) Run() {
	for {
		obj := cl.Get()
		if obj != nil {
			cl.MirrorQuery(obj.Args,obj.Server,obj.Client)
		} else if !cl.Running {
			break
		}
		time.Sleep(10 * time.Microsecond)
	}
	for _,v := range cl.DBNodes {
		v.Client.Close()
	}
	cl.DBNodes = nil
}

func (cl *ServerClient) Close() {
	cl.Running = false
}

func (cl *ServerClient) CheckDBNodes() {
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

func (cl *ServerClient) MirrorQuery(args []string,runningServer string,Client string) {	
	if len(args) > 0 {
		process := 0
		cl.CheckDBNodes()
		errflag := false
		var errMsg error
		for _,v := range cl.DBNodes {
			db := v.Client
		    if v.Info.Mode != "queries" && v.Info.Id != runningServer {
		    
		   		val,err := db.Do(args)
			   	if err != nil {
			   		errMsg = err
			   		if CONFIGS.Sync { 
				   		log.Println("mirror query error args:",args," Do Response:",val,"error:",err)
				   	}
			   		errflag = true
			   		continue	
			   	}
			   	
			   	if val[0] == "ok" {
		    		process++
		    	} 
	    	}
	    }
		
		//if some date save failed,we will retry again.
		if errflag {
			cl.Append(args,runningServer,Client)
			log.Println("mirror query failed args:",args,"error:",errMsg)
		}
		if CONFIGS.Sync { 
			log.Printf("MirrorQuery Args:%v Info:%v Process:%d\n",args,runningServer,process)
		}	
	}	
}