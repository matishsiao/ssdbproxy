package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

var heapProfileCounter int32
var startTime = time.Now()
var pid int

func init() {
	pid = os.Getpid()
}

func StartCPUProfile() {
	f, err := os.Create("cpu-" + strconv.Itoa(pid) + ".pprof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
}

func StopCPUProfile() {
	pprof.StopCPUProfile()
}

func StartBlockProfile(rate int) {
	runtime.SetBlockProfileRate(rate)
}

func StopBlockProfile() {
	filename := "block-" + strconv.Itoa(pid) + ".pprof"
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	if err = pprof.Lookup("block").WriteTo(f, 0); err != nil {
		log.Fatalf(" can't write %s: %s", filename, err)
	}
	f.Close()
}

func SetMemProfileRate(rate int) {
	runtime.MemProfileRate = rate
}

func GC() {
	runtime.GC()
}

func DumpHeap() {
	filename := "heap-" + strconv.Itoa(pid) + "-" + strconv.Itoa(int(atomic.AddInt32(&heapProfileCounter, 1))) + ".pprof"
	f, err := os.Create(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "testing: %s", err)
		return
	}
	if err = pprof.WriteHeapProfile(f); err != nil {
		fmt.Fprintf(os.Stderr, "testing: can't write %s: %s", filename, err)
	}
	f.Close()
}

func showSystemStat(interval time.Duration, count int) {

	usage1 := &syscall.Rusage{}
	var lastUtime int64
	var lastStime int64

	counter := 0
	for {

		//http://man7.org/linux/man-pages/man3/vtimes.3.html
		syscall.Getrusage(syscall.RUSAGE_SELF, usage1)

		utime := usage1.Utime.Nano()
		stime := usage1.Stime.Nano()
		userCPUUtil := float64(utime-lastUtime) * 100 / float64(interval)
		sysCPUUtil := float64(stime-lastStime) * 100 / float64(interval)
		memUtil := usage1.Maxrss * 1024

		lastUtime = utime
		lastStime = stime

		if counter > 0 {
			fmt.Printf("cpu: %3.2f%% us  %3.2f%% sy, mem:%s \n", userCPUUtil, sysCPUUtil, toH(uint64(memUtil)))
		}

		counter += 1
		if count >= 1 && count < counter {
			return
		}
		time.Sleep(interval)
	}

}

func ShowSystemStat(seconds int) {
	go func() {
		interval := time.Duration(seconds) * time.Second
		showSystemStat(interval, 0)
	}()
}

func PrintSystemStats() {
	interval := time.Duration(1) * time.Second
	showSystemStat(interval, 1)
}

func ShowGCStat() {
	go func() {
		var numGC int64

		interval := time.Duration(100) * time.Millisecond
		gcstats := &debug.GCStats{PauseQuantiles: make([]time.Duration, 100)}
		memStats := &runtime.MemStats{}
		for {
			debug.ReadGCStats(gcstats)
			if gcstats.NumGC > numGC {
				runtime.ReadMemStats(memStats)

				printGC(memStats, gcstats)
				numGC = gcstats.NumGC
			}
			time.Sleep(interval)
		}
	}()
}

func PrintGCSummary() {
	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)
	gcstats := &debug.GCStats{PauseQuantiles: make([]time.Duration, 100)}
	debug.ReadGCStats(gcstats)

	printGC(memStats, gcstats)
}




func printGC(memStats *runtime.MemStats, gcstats *debug.GCStats) {

	if gcstats.NumGC > 0 {
		lastPause := gcstats.Pause[0]
		elapsed := time.Now().Sub(startTime)
		overhead := float64(gcstats.PauseTotal) / float64(elapsed) * 100
		allocatedRate := float64(memStats.TotalAlloc) / elapsed.Seconds()
		log.Printf("[GC-Enabled]Connection:%d Goroutine:%d NumGC:%d Pause:%s Pause(Avg):%s Overhead:%3.2f%% Alloc:%s Sys:%s Alloc(Rate):%s/s Histogram:%s %s %s \n",
			ProxyConn,
			runtime.NumGoroutine(),
			gcstats.NumGC,
			toS(lastPause),
			toS(avg(gcstats.Pause)),
			overhead,
			toH(memStats.Alloc),
			toH(memStats.Sys),
			toH(uint64(allocatedRate)),
			toS(gcstats.PauseQuantiles[94]),
			toS(gcstats.PauseQuantiles[98]),
			toS(gcstats.PauseQuantiles[99]))
	} else {
		// while GC has disabled
		elapsed := time.Now().Sub(startTime)
		allocatedRate := float64(memStats.TotalAlloc) / elapsed.Seconds()

		log.Printf("[GC-Disabled]Connection:%d Goroutine:%d Alloc:%s Sys:%s Alloc(Rate):%s/s\n",
			ProxyConn,
			runtime.NumGoroutine(),
			toH(memStats.Alloc),
			toH(memStats.Sys),
			toH(uint64(allocatedRate)))
	}
}

func avg(items []time.Duration) time.Duration {
	var sum time.Duration
	for _, item := range items {
		sum += item
	}
	return time.Duration(int64(sum) / int64(len(items)))
}

// human readable format
func toH(bytes uint64) string {
	switch {
	case bytes < 1024:
		return fmt.Sprintf("%dB", bytes)
	case bytes < 1024*1024:
		return fmt.Sprintf("%.2fK", float64(bytes)/1024)
	case bytes < 1024*1024*1024:
		return fmt.Sprintf("%.2fM", float64(bytes)/1024/1024)
	default:
		return fmt.Sprintf("%.2fG", float64(bytes)/1024/1024/1024)
	}
}

// short string format
func toS(d time.Duration) string {

	u := uint64(d)
	if u < uint64(time.Second) {
		switch {
		case u == 0:
			return "0"
		case u < uint64(time.Microsecond):
			return fmt.Sprintf("%.2fns", float64(u))
		case u < uint64(time.Millisecond):
			return fmt.Sprintf("%.2fus", float64(u)/1000)
		default:
			return fmt.Sprintf("%.2fms", float64(u)/1000/1000)
		}
	} else {
		switch {
		case u < uint64(time.Minute):
			return fmt.Sprintf("%.2fs", float64(u)/1000/1000/1000)
		case u < uint64(time.Hour):
			return fmt.Sprintf("%.2fm", float64(u)/1000/1000/1000/60)
		default:
			return fmt.Sprintf("%.2fh", float64(u)/1000/1000/1000/60/60)
		}
	}

}
