package main

import (
	"fmt"
	"time"
	"io/ioutil"
	"encoding/json"
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
   fmt.Println(mt)
}

func dump_wal(s string) {
	for (true){
		time.Sleep(wal_interval * time.Millisecond)
		file, _ := json.MarshalIndent(wal, "", " ")
 		_ = ioutil.WriteFile("wal_file.json", file, 0644)
		fmt.Println(s)
		fmt.Println(mt)
	}
}

func dump_it(s string) {
	file, _ := json.MarshalIndent(mt, "", " ")
	_ = ioutil.WriteFile("index_table.json", file, 0644)
}
