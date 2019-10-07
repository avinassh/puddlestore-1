// Brown University, CS138, Spring 2018
//
// Purpose: Implements functions that are invoked by clients and other nodes
// over RPC.

package raft

import "golang.org/x/net/context"

// JoinCaller is called through GRPC to execute a join request.
func (local *RaftNode) JoinCaller(ctx context.Context, r *RemoteNode) (*Ok, error) {
	// Check if the network policy prevents incoming requests from the requesting node
	if local.NetworkPolicy.IsDenied(*r, *local.GetRemoteSelf()) {
		return nil, ErrorNetworkPolicyDenied
	}

	// Defer to the local Join implementation, and marshall the results to
	// respond to the GRPC request.
	err := local.Join(r)
	return &Ok{Ok: err == nil}, err
}

// StartNodeCaller is called through GRPC to execute a start node request.
// STUDENT WRITTEN
func (local *RaftNode) StartNodeCaller(ctx context.Context, req *StartNodeRequest) (*Ok, error) {
	if local.NetworkPolicy.IsDenied(*req.FromNode, *local.GetRemoteSelf()) {
		return nil, ErrorNetworkPolicyDenied
	}

	err := local.StartNode(req)
	return &Ok{Ok: err == nil}, err
}

// AppendEntriesCaller is called through GRPC to respond to an append entries request.
// STUDENT WRITTEN
func (local *RaftNode) AppendEntriesCaller(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesReply, error) {
	if local.NetworkPolicy.IsDenied(*req.GetLeader(), *local.GetRemoteSelf()) {
		return nil, ErrorNetworkPolicyDenied
	}

	res, err := local.AppendEntries(req)
	return &res, err
}

// RequestVoteCaller is called through GRPC to respond to a vote request.
// STUDENT WRITTEN
func (local *RaftNode) RequestVoteCaller(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
	if local.NetworkPolicy.IsDenied(*req.GetCandidate(), *local.GetRemoteSelf()) {
		return nil, ErrorNetworkPolicyDenied
	}

	res, err := local.RequestVote(req)
	return &res, err
}

// RegisterClientCaller is called through GRPC to respond to a client
// registration request.
// STUDENT WRITTEN
func (local *RaftNode) RegisterClientCaller(ctx context.Context, req *RegisterClientRequest) (*RegisterClientReply, error) {
	res, err := local.RegisterClient(req)
	return &res, err
}

// ClientRequestCaller is called through GRPC to respond to a client request.
// STUDENT WRITTEN
func (local *RaftNode) ClientRequestCaller(ctx context.Context, req *ClientRequest) (*ClientReply, error) {
	res, err := local.ClientRequest(req)
	return &res, err
}
