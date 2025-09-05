package main

import (
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
		dumpIT(s)
		singletonTable.dumpMT(s)
	}
}

func (sing *SingletonTable) dumpMT(s string) {
	sing.mu.Lock()
	file, _ := jsonIterGlobal.Marshal(sing.mt)
	_ = ioutil.WriteFile("mem_table.json", file, 0644)
	sing.mu.Unlock()
	fmt.Println(s)
	//fmt.Println(mt)
}

func dumpGeneric(fileName string, file []byte) {
	err := ioutil.WriteFile(fileName, file, 0600)
	if err != nil {
		fmt.Println(err)
	}
}

func (sing *SingletonWal) dumpWal(s string) {
	for true {
		time.Sleep(wal_interval)
		sing.mu.Lock()
		fileName := "wal_file.json"
		file, _ := jsonIterGlobal.Marshal(sing.wal)
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

func dumpIT(s string) {
	file, _ := jsonIterGlobal.Marshal(it)
	_ = ioutil.WriteFile("index_table.json", file, 0644)
}

func dumpConfig(s string) {
	file, _ := jsonIterGlobal.Marshal(configs_file)
	_ = ioutil.WriteFile("configfile.json", file, 0644)
	fmt.Println(s)
	//fmt.Println(mt)
}

func checkFreeMemory() int64 {
	return getFreeMemory()
}

func getServerFreeMemory(payload interface{}) interface{} {
	mem := checkFreeMemory()
	return mem
}
