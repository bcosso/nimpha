package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"
)

var data_interval time.Duration = (15000 * time.Millisecond)
var wal_interval time.Duration = (5000 * time.Millisecond)
var wal_limit int = 0

func dump_data(s string) {
	for true {
		time.Sleep(data_interval)
		dump_it(s)
		singletonTable.dump_mt(s)
	}
}

func (sing *SingletonTable) dump_mt(s string) {
	sing.mu.Lock()
	file, _ := json.MarshalIndent(sing.mt, "", " ")
	_ = ioutil.WriteFile("mem_table.json", file, 0644)
	sing.mu.Unlock()
	fmt.Println(s)
	//fmt.Println(mt)
}

func dump_generic(fileName string, file []byte) {
	err := ioutil.WriteFile(fileName, file, 0600)
	if err != nil {
		fmt.Println(err)
	}
}

func (sing *SingletonWal) dump_wal(s string) {
	for true {
		time.Sleep(wal_interval)
		sing.mu.Lock()
		fileName := "wal_file.json"
		file, _ := json.MarshalIndent(sing.wal, "", " ")
		if wal_limit > 0 && len(file) >= wal_limit {
			fileName = "wal_file%s.json"
			fileName = fmt.Sprintf(fileName, time.Now().Format("20060102150405"))
			sing.wal = make(map[string]wal_operation)
		}

		err := ioutil.WriteFile(fileName, file, 0644)
		if err != nil {
			fmt.Println(err)
		}
		sing.mu.Unlock()

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
