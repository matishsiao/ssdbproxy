package main 

import (
	"log"
	"flag"
	"os"
	"time"
	"io/ioutil"
	"encoding/json"
	"runtime/debug"
	"runtime"
	"runtime/pprof"
	"sync"
)
var (
	version string = "0.0.4"
	configPath string = "configs.json"
	CONFIGS Configs
	modTime time.Time
	dbClient ServerClient
	memprofile string = "mempprof.log"
	memFile *os.File
	
)

func main() {
	log.Println("Version:",version)
	flag.StringVar(&configPath,"c","configs.json","config file path")
	//flag.StringVar(&memprofile,"mm", "", "write memory profile to this file")
	flag.Parse()
	
	
	config,err := loadConfigs(configPath)
	if err != nil {
		log.Println("Load config file error:",err)
		os.Exit(1)
	}
	CONFIGS = config
	SetUlimit(102000)
	debug.SetGCPercent(50)
	useCPU := runtime.NumCPU() - 1
	if useCPU <= 0 {
		useCPU = 1
	}
	runtime.GOMAXPROCS(useCPU)
	//go memPorfile()
	dbClient = ServerClient{Mutex:&sync.Mutex{},Running:true}
	dbClient.ArgsChannel = make(chan []string)
	go dbClient.Serve()
	go Listen(CONFIGS.Host,CONFIGS.Port)
	for {
		configWatcher()
		time.Sleep(250 * time.Millisecond)
	}
}

func memPorfile() {
	if memprofile != "" {
        var err error
        memFile, err = os.Create(memprofile)
        if err != nil {
            log.Println(err)
        } else {
            log.Println("start write heap profile")
            pprof.WriteHeapProfile(memFile)
            defer memFile.Close()
        }
    }
	writeMemProfile()
    time.Sleep(300 * time.Second)
    log.Println("write heap profile finished.")
}
func writeMemProfile() {
    pprof.WriteHeapProfile(memFile)
}

func configWatcher() {
	file, err := os.Open(configPath) // For read access.
	if err != nil {
		log.Println("configWatcher error:",err)
	}
	info, err := file.Stat()
	if err != nil {
		log.Println("configWatcher error:",err)
	}
	if modTime.Unix() == -62135596800 {
		log.Println("configWatcher init mod time")
		modTime = info.ModTime()
	}

	if info.ModTime() != modTime {
		log.Printf("Config file changed. Reolad config file.\n")
		modTime = info.ModTime()
		CONFIGS,err = loadConfigs(configPath)
		if err != nil {			
			log.Printf("configWatcher error:%v\n",err)
		}
	}
	defer file.Close()
}

func loadConfigs(fileName string) (Configs,error) {
	file, e := ioutil.ReadFile(fileName)
	if e != nil {
		log.Printf("Load config file error: %v\n", e)
		os.Exit(1)
	}
	
	var config Configs
	err := json.Unmarshal(file, &config)
	if err != nil {
		log.Printf("Config load error:%v \n",err)
		return config,err
	}
	return config,nil
}

