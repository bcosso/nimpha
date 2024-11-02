//go:build !cgo

package main

func getFreeMemory() int64 {
	panic("CGO NOT FOUND. Gcc not installed in the build server")
}
