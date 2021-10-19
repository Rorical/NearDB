package main

import "github.com/Rorical/NearDB/src/rpc"

func main() {
	ser, err := rpc.NewService()
	if err != nil {
		panic(err)
	}
	rpc.RunService(":9888", ser)
}
