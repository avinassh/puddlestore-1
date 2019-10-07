package puddlestore

import (

	"io/ioutil"
	"log"
	"os"

	"google.golang.org/grpc/grpclog"
)

var Debug *log.Logger
var Out *log.Logger
var Error *log.Logger

// Initialize the loggers
func init() {
	Debug = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	Out = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
	Error = log.New(os.Stderr, "ERROR: ", log.Ltime|log.Lshortfile)

	grpclog.SetLogger(log.New(ioutil.Discard, "", 0))
}

// SetDebug turns printing debugChan strings on or off
func SetDebug(enabled bool) {
	if enabled {
		Debug.SetOutput(os.Stdout)
	} else {
		Debug.SetOutput(ioutil.Discard)
	}
}