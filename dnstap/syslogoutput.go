/* Modification of TextOutput for sending to syslog
 */

package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	dnstap ".."
)

// Output channel buffer size value from main dnstap package.
// const outputChannelSize = 32

//
// A syslogOutput implements a dnstap.Output which writes frames to a file
// and closes and reopens the file on SIGHUP.
//
// Data frames are written in binary fstrm format unless a text formatting
// function (dnstp.TextFormatFunc) is given or the filename is blank or "-".
// In the latter case, data is written in compact (quiet) text format unless
// an alternate text format is given on the assumption that stdout is a terminal.
//
type syslogOutput struct {
	formatter dnstap.TextFormatFunc
	output    dnstap.Output
	data      chan []byte
	done      chan struct{}
}

func openSyslog(formatter dnstap.TextFormatFunc) (o dnstap.Output, err error) {
	if formatter != nil {
		o, err = dnstap.NewSyslogOutput(formatter)
	}
	return
}

func newSyslogOutput(formatter dnstap.TextFormatFunc) (*syslogOutput, error) {
	o, err := openSyslog(formatter)
	if err != nil {
		return nil, err
	}
	return &syslogOutput{
		formatter: formatter,
		output:    o,
		data:      make(chan []byte, outputChannelSize),
		done:      make(chan struct{}),
	}, nil
}

func (so *syslogOutput) GetOutputChannel() chan []byte {
	return so.data
}

func (so *syslogOutput) Close() {
	close(so.data)
	<-so.done
}

func (so *syslogOutput) RunOutputLoop() {
	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt, syscall.SIGHUP)
	o := so.output
	go o.RunOutputLoop()
	defer func() {
		o.Close()
		close(so.done)
	}()
	for {
		select {
		case b, ok := <-so.data:
			if !ok {
				return
			}
			o.GetOutputChannel() <- b
		case sig := <-sigch:
			if sig == syscall.SIGHUP {
				o.Close()
				newo, err := openSyslog(so.formatter)
				if err != nil {
					fmt.Fprintf(os.Stderr,
						"dnstap: Error: Syslog open failed %s\n",
						err)
					os.Exit(1)
				}
				o = newo
				go o.RunOutputLoop()
				continue
			}
			os.Exit(0)
		}
	}
}
