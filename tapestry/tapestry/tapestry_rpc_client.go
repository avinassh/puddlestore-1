/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: Provides wrappers around the client interface of GRPC to invoke
 *  functions on remote tapestry nodes.
 */

package tapestry

import (
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	// Uncomment for xtrace
	// util "github.com/brown-csci1380/tracing-framework-go/xtrace/grpcutil"
)

var dialOptions []grpc.DialOption

func init() {
	dialOptions = []grpc.DialOption{grpc.WithInsecure(), grpc.FailOnNonTempDialError(true)}
	// Uncomment for xtrace
	// dialOptions = append(dialOptions, grpc.WithUnaryInterceptor(util.XTraceClientInterceptor))
}

type RemoteNode struct {
	Id      ID
	Address string
}

// Equals implements equality for RemoteNodes
func (rn *RemoteNode) Equals(node RemoteNode) bool {
	return rn.Id.String() == node.Id.String() && rn.Address == node.Address
}

// ContainedIn returns true if the node is in the given list
func (rn *RemoteNode) ContainedIn(nodes []RemoteNode) bool {
	for _, n := range nodes {
		if rn.Equals(n) {
			return true
		}
	}
	return false
}

// Turns a NodeMsg into a RemoteNode
func (n *NodeMsg) toRemoteNode() RemoteNode {
	if n == nil {
		return RemoteNode{}
	}
	idVal, err := ParseID(n.Id)
	if err != nil {
		return RemoteNode{}
	}
	return RemoteNode{
		Id:      idVal,
		Address: n.Address,
	}
}

// Turns a RemoteNode into a NodeMsg
func (n *RemoteNode) toNodeMsg() *NodeMsg {
	if n == nil {
		return nil
	}
	return &NodeMsg{
		Id:      n.Id.String(),
		Address: n.Address,
	}
}

/**
 *  RPC invocation functions
 */

var connMap = make(map[string]*grpc.ClientConn)
var connMapLock = &sync.RWMutex{}

func closeAllConnections() {
	connMapLock.Lock()
	defer connMapLock.Unlock()
	for k, conn := range connMap {
		conn.Close()
		delete(connMap, k)
	}
}

// Creates a new client connection to the given remote node
func makeClientConn(remote *RemoteNode) (*grpc.ClientConn, error) {
	return grpc.Dial(remote.Address, dialOptions...)
}

// Creates or returns a cached RPC client for the given remote node
func (remote *RemoteNode) ClientConn() (TapestryRPCClient, error) {
	connMapLock.RLock()
	if cc, ok := connMap[remote.Address]; ok {
		connMapLock.RUnlock()
		return NewTapestryRPCClient(cc), nil
	}
	connMapLock.RUnlock()

	cc, err := makeClientConn(remote)
	if err != nil {
		return nil, err
	}
	connMapLock.Lock()
	connMap[remote.Address] = cc
	connMapLock.Unlock()

	return NewTapestryRPCClient(cc), err
}

// Remove the client connection to the given node, if present
func (remote *RemoteNode) RemoveClientConn() {
	connMapLock.Lock()
	defer connMapLock.Unlock()
	if cc, ok := connMap[remote.Address]; ok {
		cc.Close()
		delete(connMap, remote.Address)
	}
}

// Check the error and remove the client connection if necessary
func (remote *RemoteNode) connCheck(err error) error {
	if err != nil {
		remote.RemoveClientConn()
	}
	return err
}

// Say hello to a remote address, and get the tapestry node there
func SayHelloRPC(addr string, joiner RemoteNode) (RemoteNode, error) {
	remote := &RemoteNode{Address: addr}
	cc, err := remote.ClientConn()
	if err != nil {
		return RemoteNode{}, err
	}
	node, err := cc.HelloCaller(context.Background(), joiner.toNodeMsg())
	return node.toRemoteNode(), remote.connCheck(err)
}

// STUDENT WRITTEN
// Spec:
// If no connection can be made to the remote node, returns false, RemoteNode{}, non-nil err
// If connection made, call is executed and arguments unpacked
func (remote *RemoteNode) GetNextHopRPC(id ID) (bool, RemoteNode, error) {
	cc, err := remote.ClientConn()
	if err != nil {
		return false, RemoteNode{}, err
	}
	rsp, err := cc.GetNextHopCaller(context.Background(), &IdMsg{id.String()})
	if err != nil {
		return false, RemoteNode{}, err
	}
	return rsp.HasNext, rsp.Next.toRemoteNode(), remote.connCheck(err)
}

func (remote *RemoteNode) RegisterRPC(key string, replica RemoteNode) (bool, error) {
	cc, err := remote.ClientConn()
	if err != nil {
		return false, err
	}
	rsp, err := cc.RegisterCaller(context.Background(), &Registration{
		FromNode: replica.toNodeMsg(),
		Key:      key,
	})
	return rsp.GetOk(), remote.connCheck(err)
}

// STUDENT WRITTEN
// If fetch throws an error, returns false, nil err
func (remote *RemoteNode) FetchRPC(key string) (bool, []RemoteNode, error) {
	cc, err := remote.ClientConn()
	if err != nil {
		// what to do when err
	}
	req := &Key{Key: key}
	locs, err := cc.FetchCaller(context.Background(), req)
	if err != nil {
		return false, nil, remote.connCheck(err)
	}
	return locs.IsRoot, nodeMsgsToRemoteNodes(locs.Values), remote.connCheck(err)
}

func (remote *RemoteNode) RemoveBadNodesRPC(badnodes []RemoteNode) error {
	cc, err := remote.ClientConn()
	if err != nil {
		return err
	}
	_, err = cc.RemoveBadNodesCaller(context.Background(), &Neighbors{remoteNodesToNodeMsgs(badnodes)})
	// TODO(TA) deal with ok
	return remote.connCheck(err)
}

// STUDENT WRITTEN
// If AddNode throws an error, returns nil, err
func (remote *RemoteNode) AddNodeRPC(toAdd RemoteNode) ([]RemoteNode, error) {
	cc, err := remote.ClientConn()
	if err != nil {
		return nil, err
	}
	n, err := cc.AddNodeCaller(context.Background(), toAdd.toNodeMsg())
	if err != nil {
		return nil, remote.connCheck(err)
	}
	return nodeMsgsToRemoteNodes(n.Neighbors), remote.connCheck(err)
}

func (remote *RemoteNode) AddNodeMulticastRPC(newNode RemoteNode, level int) ([]RemoteNode, error) {
	cc, err := remote.ClientConn()
	if err != nil {
		return nil, err
	}
	rsp, err := cc.AddNodeMulticastCaller(context.Background(), &MulticastRequest{
		NewNode: newNode.toNodeMsg(),
		Level:   int32(level),
	})
	if err != nil {
		return nil, remote.connCheck(err)
	}
	return nodeMsgsToRemoteNodes(rsp.Neighbors), remote.connCheck(err)
}

// STUDENT WRITTEN
// If transfer throws an error,
func (remote *RemoteNode) TransferRPC(from RemoteNode, data map[string][]RemoteNode) error {
	cc, err := remote.ClientConn()
	if err != nil {
		return err
	}
	req := &TransferData{
		From: from.toNodeMsg(),
		Data: make(map[string]*Neighbors),
	}

	//populates req
	for key, nodes := range data {
		req.Data[key] = &Neighbors{
			Neighbors: remoteNodesToNodeMsgs(nodes),
		}
	}

	_, err2 := cc.TransferCaller(context.Background(), req)
	return remote.connCheck(err2)
}

func (remote *RemoteNode) AddBackpointerRPC(bp RemoteNode) error {
	cc, err := remote.ClientConn()
	if err != nil {
		return err
	}
	_, err = cc.AddBackpointerCaller(context.Background(), bp.toNodeMsg())
	// TODO deal with Ok
	return remote.connCheck(err)
}

// STUDENT WRITTEN
func (remote *RemoteNode) RemoveBackpointerRPC(bp RemoteNode) error {
	cc, err := remote.ClientConn()
	if err != nil {
		return err
	}
	_, err2 := cc.RemoveBackpointerCaller(context.Background(), bp.toNodeMsg())
	return remote.connCheck(err2)
}

func (remote *RemoteNode) GetBackpointersRPC(from RemoteNode, level int) ([]RemoteNode, error) {
	cc, err := remote.ClientConn()
	if err != nil {
		return nil, err
	}
	rsp, err := cc.GetBackpointersCaller(context.Background(), &BackpointerRequest{from.toNodeMsg(), int32(level)})
	if err != nil {
		return nil, remote.connCheck(err)
	}
	return nodeMsgsToRemoteNodes(rsp.Neighbors), remote.connCheck(err)
}

// STUDENT WRITTEN
func (remote *RemoteNode) NotifyLeaveRPC(from RemoteNode, replacement *RemoteNode) error {
	cc, err := remote.ClientConn()
	if err != nil {
		return err
	}

	req := &LeaveNotification{
		From:        from.toNodeMsg(),
		Replacement: (*replacement).toNodeMsg(),
	}
	_, err2 := cc.NotifyLeaveCaller(context.Background(), req)
	return remote.connCheck(err2)
}

func (remote *RemoteNode) BlobStoreFetchRPC(key string) (*[]byte, error) {
	cc, err := remote.ClientConn()
	if err != nil {
		return nil, err
	}
	rsp, err := cc.BlobStoreFetchCaller(context.Background(), &Key{key})
	if err != nil {
		return nil, remote.connCheck(err)
	}
	return &rsp.Data, remote.connCheck(err)
}

func (remote *RemoteNode) TapestryLookupRPC(key string) ([]RemoteNode, error) {
	cc, err := remote.ClientConn()
	if err != nil {
		return nil, err
	}
	rsp, err := cc.TapestryLookupCaller(context.Background(), &Key{key})
	if err != nil {
		return nil, remote.connCheck(err)
	}
	return nodeMsgsToRemoteNodes(rsp.Neighbors), remote.connCheck(err)
}

func (remote *RemoteNode) TapestryStoreRPC(key string, value []byte) error {
	cc, err := remote.ClientConn()
	if err != nil {
		return err
	}
	_, err = cc.TapestryStoreCaller(context.Background(), &DataBlob{
		Key:  key,
		Data: value,
	})
	return remote.connCheck(err)
}
