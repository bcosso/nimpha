package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"
	// "syscall"
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

func checkFreeMemory() int64 {
	return getFreeMemory()
}

/*
#include <stdio.h>
#include <unistd.h>

unsigned long getTotalSystemMemory()
{
    long pages = sysconf(_SC_AVPHYS_PAGES);
    long page_size = sysconf(_SC_PAGE_SIZE);
    return pages * page_size;
}
*/

func get_server_free_memory(payload interface{}) interface{} {
	mem := checkFreeMemory()
	return mem
}
