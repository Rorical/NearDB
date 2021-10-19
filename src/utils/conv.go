package utils

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

func StringOut(bye []byte) string {
	return *(*string)(unsafe.Pointer(&bye))
}

func StringIn(strings string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&strings))
	return *(*[]byte)(unsafe.Pointer(&[3]uintptr{x[0], x[1], x[1]}))
}

func StringListToBytesList(strlist []string) [][]byte {
	result := make([][]byte, len(strlist))
	for i, str := range strlist {
		result[i] = StringIn(str)
	}
	return result
}

func IntsToBytes(n []int) []byte {
	bytebuf := bytes.NewBuffer([]byte{})
	for _, v := range n {
		data := int64(v)
		binary.Write(bytebuf, binary.BigEndian, data)
	}
	return bytebuf.Bytes()
}

func BytesToInts(bys []byte) []int {
	bytebuff := bytes.NewBuffer(bys)
	var data []int
	var buff int64
	var e error
	for e = binary.Read(bytebuff, binary.BigEndian, &buff) ; e == nil; e = binary.Read(bytebuff, binary.BigEndian, &buff) {
		data = append(data, int(buff))
	}
	return data
}