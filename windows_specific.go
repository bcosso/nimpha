//go:build windows && cgo

package main

/*
#include <windows.h>
#include <stdio.h>

int DIV = 1048576;

long getMemoryUsageWindows(){
	  MEMORYSTATUSEX statex;

  statex.dwLength = sizeof (statex);

  GlobalMemoryStatusEx (&statex);
  return statex.ullAvailPhys/DIV
}

*/
import (
	"C"
)

func getFreeMemory() int64 {
	returnValue := C.getMemoryUsageWindows()
	goInt := int64(returnValue)
	return goInt
}
