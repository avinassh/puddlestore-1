/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: sets up several loggers and provides utility methods for printing
 *  tapestry structures.
 */

package tapestry

//__BEGIN_TA__
import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
	"google.golang.org/grpc/grpclog"
)

//__END_TA__
/*__BEGIN_STUDENT__
import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"google.golang.org/grpc/grpclog"
	// Uncomment for xtrace
	// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
)
__END_STUDENT__*/

var Debug *log.Logger
var Out *log.Logger
var Error *log.Logger
var Trace *log.Logger

// Initialize the loggers
func init() {
	Debug = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	Trace = log.New(ioutil.Discard, "", log.Lshortfile)
	//__BEGIN_TA__
	//__END_TA__
	/*__BEGIN_STUDENT__
	// Uncomment for xtrace
	// Trace.SetOutput(xtr.MakeWriter())
	__END_STUDENT__ */
	Out = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
	Error = log.New(os.Stderr, "ERROR: ", log.Ltime|log.Lshortfile)
	grpclog.SetLogger(log.New(ioutil.Discard, "", log.Ltime))
}

// Turn debug on or off
func SetDebug(enabled bool) {
	if enabled {
		Debug.SetOutput(os.Stdout)
		// uncomment for xtrace
		// Debug.SetOutput(xtr.MakeWriter(os.Stdout))
	} else {
		Debug.SetOutput(ioutil.Discard)
	}
}

// Stringifies the routing table
func (tapestry *Node) RoutingTableToString() string {
	var buffer bytes.Buffer
	table := tapestry.table
	fmt.Fprintf(&buffer, "RoutingTable for node %v\n", tapestry.node)
	id := table.local.Id.String()
	for i, row := range table.rows {
		for j, slot := range row {
			for _, node := range *slot {
				fmt.Fprintf(&buffer, " %v%v  %v: %v %v\n", id[:i], strings.Repeat(" ", DIGITS-i+1), Digit(j), node.Address, node.Id.String())
			}
		}
	}

	return buffer.String()
}

// Prints the routing table
func (tapestry *Node) PrintRoutingTable() {
	fmt.Println(tapestry.RoutingTableToString())
}

// Stringifies the location map
func (tapestry *Node) LocationMapToString() string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "LocationMap for node %v\n", tapestry.node)
	for key, values := range tapestry.locationsByKey.data {
		fmt.Fprintf(&buffer, " %v: %v\n", key, slice(values))
	}

	return buffer.String()
}

// Prints the location map
func (tapestry *Node) PrintLocationMap() {
	fmt.Printf(tapestry.LocationMapToString())
}

// Stringifies the backpointers
func (tapestry *Node) BackpointersToString() string {
	var buffer bytes.Buffer
	bp := tapestry.backpointers
	fmt.Fprintf(&buffer, "Backpointers for node %v\n", tapestry.node)
	for i, set := range bp.sets {
		for _, node := range set.Nodes() {
			fmt.Fprintf(&buffer, " %v %v: %v\n", i, node.Address, node.Id.String())
		}
	}

	return buffer.String()
}

// Prints the backpointers
func (tapestry *Node) PrintBackpointers() {
	fmt.Printf(tapestry.BackpointersToString())
}

// Stringifies the blob store
func (tapestry *Node) BlobStoreToString() string {
	var buffer bytes.Buffer
	for k, _ := range tapestry.blobstore.blobs {
		fmt.Fprintln(&buffer, k)
	}
	return buffer.String()
}

// Prints the blobstore
func (tapestry *Node) PrintBlobStore() {
	fmt.Printf(tapestry.BlobStoreToString())
}
