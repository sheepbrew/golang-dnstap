/* Modification of TextOutput for sending to syslog
 */

package dnstap

import (
	"log"
	"log/syslog"

	"github.com/golang/protobuf/proto"
)

// SyslogOutput implements a dnstap Output rendering dnstap data as text.
type SyslogOutput struct {
	format        TextFormatFunc
	outputChannel chan []byte
	wait          chan bool
	writer        *syslog.Writer
}

// NewSyslogOutput creates a TextOutput writing dnstap data to the given io.Writer
// in the text format given by the TextFormatFunc format.
func NewSyslogOutput(format TextFormatFunc) (o *SyslogOutput, err error) {
	o = new(SyslogOutput)
	o.format = format
	o.outputChannel = make(chan []byte, outputChannelSize)
	o.wait = make(chan bool)
	l, err := syslog.New(syslog.LOG_NOTICE, "dnstap")
	if err == nil {
		o.writer = l
		return o, nil
	}
	return nil, err
}

// GetOutputChannel returns the channel on which the TextOutput accepts dnstap data.
//
// GetOutputChannel satisfies the dnstap Output interface.
func (o *SyslogOutput) GetOutputChannel() chan []byte {
	return o.outputChannel
}

// RunOutputLoop receives dnstap data sent on the output channel, formats it
// with the configured TextFormatFunc, and writes it to the file or io.Writer
// of the TextOutput.
//
// RunOutputLoop satisfies the dnstap Output interface.
func (o *SyslogOutput) RunOutputLoop() {
	dt := &Dnstap{}
	for frame := range o.outputChannel {
		if err := proto.Unmarshal(frame, dt); err != nil {
			log.Fatalf("dnstap.TextOutput: proto.Unmarshal() failed: %s\n", err)
			break
		}
		buf, ok := o.format(dt)
		if !ok {
			log.Fatalf("dnstap.TextOutput: text format function failed\n")
			break
		}
		if _, err := o.writer.Write(buf); err != nil {
			log.Fatalf("dnstap.TextOutput: write failed: %s\n", err)
			break
		}
	}
	close(o.wait)
}

// Close closes the output channel and returns when all pending data has been
// written.
//
// Close satisfies the dnstap Output interface.
func (o *SyslogOutput) Close() {
	close(o.outputChannel)
	<-o.wait
}
