package main 

import (
	"log"
)
var (
	version string = "0.0.1"
)

func main() {
	log.Println("Version:",version)
	Listen("127.0.0.1",4001)
}

