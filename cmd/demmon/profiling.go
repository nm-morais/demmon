package main

// import (
// 	"log"
// 	"os"
// 	"path/filepath"
// 	"runtime"
// 	"runtime/pprof"
// )

// func memProfile(path, fileName string) {
// 	_ = os.MkdirAll(path, os.ModePerm)
// 	profilingFilePath := filepath.Join(path, fileName)
// 	f, err := os.Create(profilingFilePath)
// 	if err != nil {
// 		log.Fatal("could not create memory profile: ", err)
// 	}
// 	runtime.GC() // get up-to-date statistics
// 	if err := pprof.WriteHeapProfile(f); err != nil {
// 		f.Close()
// 		log.Fatal("could not write memory profile: ", err)
// 	}
// 	f.Close()
// }
