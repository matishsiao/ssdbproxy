package main 

import (
	"log"
	"flag"
	"os"
	"time"
	"io/ioutil"
	"encoding/json"
)
var (
	version string = "0.0.1"
	configPath string = "configs.json"
	CONFIGS Configs
	modTime time.Time
)

func main() {
	log.Println("Version:",version)
	flag.StringVar(&configPath,"c","configs.json","config file path")
	config,err := loadConfigs(configPath)
	if err != nil {
		log.Println("Load config file error:",err)
		os.Exit(1)
	}
	CONFIGS = config
	SetUlimit(102000)
	go Listen(CONFIGS.Host,CONFIGS.Port)
	for {
		configWatcher()
		time.Sleep(250 * time.Millisecond)
	}
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

