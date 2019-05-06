package main

import (
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"

	"github.com/luw2007/redcon"
)

var addr = ":6379"

func StartCpuProf() {
	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Panic("prof err", err)
	}
	err = pprof.StartCPUProfile(f)
	if err != nil {
		log.Panic("prof err", err)
	}
}

func StopCpuProf() {
	pprof.StopCPUProfile()
}

func SaveMemProf() {
	f, err := os.Create("mem.prof")
	if err != nil {
		log.Panic("mem prof err", err)
	}
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Panic("mem prof err", err)
	}
	f.Close()
}

func main() {
	var mu sync.RWMutex
	var items = make(map[string][]byte)
	runtime.GOMAXPROCS(runtime.NumCPU())
	go log.Printf("started server at %s", addr)

	err := redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd *redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			case "get":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.RLock()
				val, ok := items[string(cmd.Args[1])]
				mu.RUnlock()
				if ok {
					conn.WriteBulk(val)
				} else {
					conn.WriteNull()
				}

			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.Lock()
				items[string(cmd.Args[1])] = cmd.Args[2]
				mu.Unlock()
				conn.WriteString("OK")
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "detach":
				hconn := conn.Detach()
				log.Printf("connection has been detached")
				go func() {
					defer hconn.Close()
					hconn.WriteString("OK")
					hconn.Flush()
				}()
				return
			case "start":
				StartCpuProf()
			case "stop":
				SaveMemProf()
				StopCpuProf()
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()

			case "del":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.Lock()
				_, ok := items[string(cmd.Args[1])]
				delete(items, string(cmd.Args[1]))
				mu.Unlock()
				if !ok {
					conn.WriteInt(0)
				} else {
					conn.WriteInt(1)
				}
			}
		},
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			//log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
