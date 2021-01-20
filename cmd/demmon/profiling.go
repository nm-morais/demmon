package main

import (
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
)

func memProfile(path, fileName string) {
	os.MkdirAll(path, os.ModePerm)
	profilingFilePath := filepath.Join(path, fileName)
	f, err := os.Create(profilingFilePath)
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}
	defer f.Close() // error handling omitted for example
	runtime.GC()    // get up-to-date statistics
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatal("could not write memory profile: ", err)
	}
}
