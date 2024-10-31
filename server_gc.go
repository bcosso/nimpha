package main

import (
	"fmt"
	"time"
	"io/ioutil"
	"encoding/json"
	"runtime"
	"syscall"
)

const data_interval = 10000
const wal_interval = 1000

func dump_data(s string) {
	for (true){
		time.Sleep(data_interval * time.Millisecond)
		dump_it(s)
		dump_mt(s)
	}
}

func dump_mt(s string) {
	file, _ := json.MarshalIndent(mt, "", " ")
	_ = ioutil.WriteFile("mem_table.json", file, 0644)
   fmt.Println(s)
   //fmt.Println(mt)
}

func dump_wal(s string) {
	for (true){
		time.Sleep(wal_interval * time.Millisecond)
		file, _ := json.MarshalIndent(wal, "", " ")
 		_ = ioutil.WriteFile("wal_file.json", file, 0644)
		fmt.Println(s)
		//fmt.Println(mt)
	}
}

func dump_it(s string) {
	file, _ := json.MarshalIndent(it, "", " ")
	_ = ioutil.WriteFile("index_table.json", file, 0644)
}

func check_os() uintptr{
	var result uintptr
	switch runtime.GOOS{
	case "windows":
		result = getWindowsMemory()
		break
	default:
		break
	}
	return result
}

func getWindowsMemory() uintptr{
	memtest, errFindingLib        := syscall.LoadLibrary("MemoryDiag.dll")
		if (errFindingLib != nil){
			panic(errFindingLib)
		}

		getModuleHandle, errFindingMethod := syscall.GetProcAddress(memtest, "getMemory")
		if (errFindingMethod != nil){
			panic(errFindingMethod)
		}
		var nargs uintptr = 0
		if ret, _, callErr := syscall.Syscall(uintptr(getModuleHandle), nargs, 0, 0, 0); callErr != 0 {
			panic(callErr)
		} else {
			fmt.Println("Result:")
			fmt.Println(ret)
			return ret
		}
}

func get_server_free_memory(payload interface{}) interface{}{
	mem:=check_os()
	return mem
}


