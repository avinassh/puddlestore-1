/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: sets up several loggers.
 */

package liteminer

import (
	"io/ioutil"
	"log"
	"os"
)

var Debug *log.Logger
var Out *log.Logger
var Err *log.Logger

// init initializes the loggers.
func init() {
	Debug = log.New(ioutil.Discard, "DEBUG: ", log.Ltime|log.Lshortfile)
	Out = log.New(os.Stdout, "INFO: ", log.Ltime|log.Lshortfile)
	Err = log.New(os.Stderr, "ERROR: ", log.Ltime|log.Lshortfile)
}

// SetDebug turns debug print statements on or off.
func SetDebug(enabled bool) {
	if enabled {
		Debug.SetOutput(os.Stdout)
	} else {
		Debug.SetOutput(ioutil.Discard)
	}
}
