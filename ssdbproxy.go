package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	_ "runtime/debug"
	"runtime/pprof"
	_ "sync"
	"time"

	_ "github.com/matishsiao/gossdb/ssdb"
	"github.com/shirou/gopsutil/process"
)

var (
	version      string = "0.0.17"
	configPath   string = "configs.json"
	CONFIGS      Configs
	modTime      time.Time
	GlobalClient ServerClient
	memprofile   string = "mempprof.log"
	memFile      *os.File
	full         bool
	calcCPU      bool
	useCPU       int
)

func main() {
	log.Println("Version:", version)
	flag.StringVar(&configPath, "c", "configs.json", "config file path")
	flag.BoolVar(&full, "f", false, "using full cpu")
	//flag.StringVar(&memprofile,"mm", "", "write memory profile to this file")
	flag.Parse()
	config, err := loadConfigs(configPath)
	if err != nil {
		log.Println("Load config file error:", err)
		os.Exit(1)
	}
	CONFIGS = config
	SetUlimit(1002000)
	useCPU = runtime.NumCPU()
	if !full {
		useCPU -= 1
		if useCPU <= 0 {
			useCPU = 1
		}
	}
	runtime.GOMAXPROCS(useCPU)
	go getCPU()
	GlobalClient.Init()
	go Listen(CONFIGS.Host, CONFIGS.Port)
	go WebServer()
	go checkConfig()
	for {
		PrintGCSummary()
		time.Sleep(60 * time.Second)
	}
}

func getCPU() {
	stats, _ := process.NewProcess(int32(os.Getpid()))
	for {
		cpu, _ := stats.Percent(1 * time.Second)
		if int(cpu) > useCPU*80 {
			log.Printf("CPU using too high:%v\n", cpu)
			go WatchCPU()
		}

		time.Sleep(1 * time.Second)
	}
}

func checkConfig() {
	for {
		configWatcher()
		time.Sleep(250 * time.Millisecond)
	}
}

func writeMemProfile() {
	pprof.WriteHeapProfile(memFile)
}

func configWatcher() {
	file, err := os.Open(configPath) // For read access.
	if err != nil {
		log.Println("configWatcher error:", err)
	}
	info, err := file.Stat()
	if err != nil {
		log.Println("configWatcher error:", err)
	}
	if modTime.Unix() == -62135596800 {
		log.Println("configWatcher init mod time")
		modTime = info.ModTime()
	}

	if info.ModTime() != modTime {
		log.Printf("Config file changed. Reolad config file.\n")
		modTime = info.ModTime()
		CONFIGS, err = loadConfigs(configPath)
		if err != nil {
			log.Printf("configWatcher error:%v\n", err)
		}
	}
	defer file.Close()
}

func loadConfigs(fileName string) (Configs, error) {
	file, e := ioutil.ReadFile(fileName)
	if e != nil {
		log.Printf("Load config file error: %v\n", e)
		os.Exit(1)
	}

	var config Configs
	err := json.Unmarshal(file, &config)
	if err != nil {
		log.Printf("Config load error:%v \n", err)
		return config, err
	}
	return config, nil
}
