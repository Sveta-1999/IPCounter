package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"ipcounter/bucket"
	"ipcounter/concurrent"
	"ipcounter/naive"
)

func main() {
	impl := flag.String("impl", "bucket", "counter impl: naive|concurrent|bucket")
	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: ipcounter [-impl naive|concurrent|bucket] <filename>")
		os.Exit(1)
	}
	filename := flag.Arg(0)

	var (
		count int64
		err   error
	)

	switch *impl {
	case "naive":
		count, err = naive.New().CountUniqueIPs(filename)
	case "concurrent":
		count, err = concurrent.New().CountUniqueIPs(filename)
	case "bucket":
		count, err = bucket.CountUniqueIPs(filename)
	default:
		log.Fatalf("unknown impl: %s", *impl)
	}
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("Unique IPv4 addresses: %d\n", count)
}
