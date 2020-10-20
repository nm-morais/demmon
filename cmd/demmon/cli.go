package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/nm-morais/demmon/internal/frontend"
)

type Operation = string

const (
	nodeUpdatesOp = Operation("nodeUpdates")
	metricsOp     = Operation("metrics")
	inViewOp      = Operation("inView")
)

var operations = []Operation{metricsOp, inViewOp, nodeUpdatesOp}

func readOp(reader *bufio.Reader) Operation {
	fmt.Print("-> ")
	text, _ := reader.ReadString('\n')
	// convert CRLF to LF
	text = strings.Replace(text, "\n", "", -1)
	return Operation(text)
}

func printOps(f io.Writer) {
	fmt.Fprintln(f, "Operations:")
	for i, op := range operations {
		fmt.Fprintf(f, "%d) : %+v\n", i, op)
	}
}

func printNodeUpdates(nodeUps, nodeDowns chan frontend.NodeUpdates) {
	f := bufio.NewWriter(os.Stdout)
	for {
		select {
		case nodeUpdate := <-nodeUps:
			fmt.Fprintln(f, "NODE UP")
			fmt.Fprintln(f, "Node up: ", nodeUpdate.Node)
			fmt.Fprintln(f, "View: ", nodeUpdate.View)
		case nodeUpdate := <-nodeDowns:
			fmt.Fprintln(f, "NODE DOWN")
			fmt.Fprintln(f, "Node down: ", nodeUpdate.Node)
			fmt.Fprintln(f, "View: ", nodeUpdate.View)
		}
		f.Flush()
	}
}

func Repl(frontend *frontend.Frontend) {
	f := bufio.NewWriter(os.Stdout)
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Demmon Shell")
	nodeUps, nodeDowns := frontend.GetPeerNotificationChans()
	go printNodeUpdates(nodeUps, nodeDowns)
	for {
		printOps(f)
		f.Flush()
		op := readOp(reader)
		switch op {

		case metricsOp:
			fmt.Fprintf(f, "%+v\n", frontend.GetActiveMetrics())

		case inViewOp:
			fmt.Fprintf(f, "%+v\n", frontend.GetInView())

		}
		f.Flush()
	}
}
