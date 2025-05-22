//go:build !windows && cgo

package main

/*
#include <stdio.h>
#include <unistd.h>

int DIV = 1048576;
int getTotalSystemMemory()
{
    long pages = sysconf(_SC_AVPHYS_PAGES);
    long page_size = sysconf(_SC_PAGE_SIZE);
    return pages * page_size/DIV;
}
*/
import (
	"C"
)

func getFreeMemory() int64 {
	//var returnValue uintptr = 0
	returnValue := C.getTotalSystemMemory()
	goInt := int64(returnValue)
	return goInt

}
