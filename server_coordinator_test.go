package main

import (
	"testing"
	"fmt"
)

func BenchmarkLoading(b *testing.B)  {
	for i := 0; i < b.N; i++ {
		fmt.Println("Bench get_mem_table:")
		get_mem_table()
    }
}

func TestLoad(t *testing.T){
	fmt.Println("Test get_mem_table:")
	get_mem_table()
}

