package raft

import (
	"bytes"
	"fmt"
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

// SetDebug turns printing debug strings on or off
func SetDebug(enabled bool) {
	if enabled {
		Debug.SetOutput(os.Stdout)
	} else {
		Debug.SetOutput(ioutil.Discard)
	}
}

// Out prints to standard output, prefaced with time and filename
func (r *RaftNode) Out(formatString string, args ...interface{}) {
	Out.Output(2, fmt.Sprintf("(%v/%v) %v", r.Id, r.State, fmt.Sprintf(formatString, args...)))
}

// Debug prints to standard output if SetDebug was called with enabled=true, prefaced with time and filename
func (r *RaftNode) Debug(formatString string, args ...interface{}) {
	Debug.Output(2, fmt.Sprintf("(%v/%v) %v", r.Id, r.State, fmt.Sprintf(formatString, args...)))
}

// Error prints to standard error, prefaced with "ERROR: ", time, and filename
func (r *RaftNode) Error(formatString string, args ...interface{}) {
	Error.Output(2, fmt.Sprintf("(%v/%v) %v", r.Id, r.State, fmt.Sprintf(formatString, args...)))
}

func (s NodeState) String() string {
	switch s {
	case FOLLOWER_STATE:
		return "follower"
	case CANDIDATE_STATE:
		return "candidate"
	case LEADER_STATE:
		return "leader"
	case JOIN_STATE:
		return "joining"
	default:
		return "unknown"
	}
}

func (r *RaftNode) String() string {
	return fmt.Sprintf("RaftNode{Id: %v, Addr: %v, State: %v}", r.Id, r.GetRemoteSelf().Addr, r.State)
}

// FormatState returns a string representation of the Raft node's state
func (r *RaftNode) FormatState() string {
	var buffer bytes.Buffer
	buffer.WriteString("Current node state:\n")

	for i, node := range r.GetNodeList() {
		buffer.WriteString(fmt.Sprintf("%v - %v", i, node))
		local := *r.GetRemoteSelf()

		if local == node {
			buffer.WriteString(" (local node)")
		}

		if r.Leader != nil && node == *r.Leader {
			buffer.WriteString(" (leader node)")
		}

		buffer.WriteString("\n")
	}

	buffer.WriteString(fmt.Sprintf("Current term: %v\n", r.GetCurrentTerm()))
	buffer.WriteString(fmt.Sprintf("Current state: %v\n", r.State))
	buffer.WriteString(fmt.Sprintf("Current commit index: %v\n", r.commitIndex))
	buffer.WriteString(fmt.Sprintf("Current next index: %v\n", r.nextIndex))
	buffer.WriteString(fmt.Sprintf("Current match index: %v\n", r.matchIndex))

	return buffer.String()
}

// FormatLogCache returns a string representation of the Raft node's log cache
func (r *RaftNode) FormatLogCache() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("Node %v LogCache:\n", r.Id))

	for _, entry := range r.logCache {
		buffer.WriteString(fmt.Sprintf(" idx:%v, term:%v\n", entry.Index, entry.TermId))
	}

	return buffer.String()
}

// FormatNodeListIds returns a string representation of IDs the list of nodes in the cluster
func (r *RaftNode) FormatNodeListIds(ctx string) string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%v (%v) r.NodeList = [", ctx, r.Id))

	nodeList := r.GetNodeList()
	for i, node := range nodeList {
		buffer.WriteString(fmt.Sprintf("%v", node.Id))
		if i < len(nodeList)-1 {
			buffer.WriteString(",")
		}
	}

	buffer.WriteString("]\n")
	return buffer.String()
}
