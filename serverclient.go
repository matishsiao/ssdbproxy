package main

import (
	"sync"
	"log"
	"github.com/matishsiao/gossdb/ssdb"
	_"time"
)

type ServerClient struct {
	Mutex *sync.Mutex
	ArgsList []ServerArgs
	DBNodes []*DBNode
	ArgsChannel chan []string
	Running bool
}
 
type ServerArgs struct {
	Args []string
}

func (cl *ServerClient) Append(args []string) {
	if CONFIGS.Debug {
		log.Println("Server Client Append:",args)
	}
	cl.ArgsChannel <- args
}

func (cl *ServerClient) Serve() {
	for args := range cl.ArgsChannel {
		cl.MirrorQuery(args)
	}
}

func (cl *ServerClient) Close() {
	cl.Running = false
}

func (cl *ServerClient) CheckDBNodes() {
	if len(cl.DBNodes) == 0 {
		for _,v := range CONFIGS.Nodelist {
			if v.Mode == "mirror" {
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
				if cv.Mode == "mirror" {
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
}

func (cl *ServerClient) MirrorQuery(args []string) {	
	if len(args) > 0 {
		process := 0
		cl.CheckDBNodes()
		errflag := false
		var errMsg error
		for _,v := range cl.DBNodes {
			db := v.Client
		    if v.Info.Mode == "mirror" {
		   		val,err := db.Do(args)
			   	if err != nil {
			   		errMsg = err
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
			cl.Append(args)
			log.Println("mirror query failed args:",args,"error:",errMsg)
		}
		if CONFIGS.Sync { 
			log.Printf("MirrorQuery Args:%v Info:%v Process:%d\n",args,process)
		}	
	}	
}