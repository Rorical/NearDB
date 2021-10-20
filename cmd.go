package main

import (
	"github.com/Rorical/NearDB/src/rpc"
	"log"
)

func main() {
	ser, err := rpc.NewService()
	if err != nil {
		panic(err)
	}
	log.Println("Running Service at :9888")
	rpc.RunService(":9888", ser)
}
