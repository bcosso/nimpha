package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"
)

const data_interval = 10000
const wal_interval = 1000

func dump_data(s string) {
	for true {
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

func dump_generic(fileName string, file []byte) {
	err := ioutil.WriteFile(fileName, file, 0600)
	if err != nil {
		fmt.Println(err)
	}
}

func dump_wal(s string) {
	for true {
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

func dump_config(s string) {
	file, _ := json.MarshalIndent(configs_file, "", " ")
	_ = ioutil.WriteFile("configfile.json", file, 0644)
	fmt.Println(s)
	//fmt.Println(mt)
}

func checkFreeMemory() int64 {
	return getFreeMemory()
}

func get_server_free_memory(payload interface{}) interface{} {
	mem := checkFreeMemory()
	return mem
}
