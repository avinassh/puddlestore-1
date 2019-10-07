package puddlestore

import (
	"errors"
	"fmt"

	"strings"

	"github.com/brown-csci1380/s18-mcisler-vmathur2/puddlestore/keyvaluestore"
	"github.com/samuel/go-zookeeper/zk"
)

/*
	Methods related to managing versioning of Inodes in PuddleStore.
	Essentially the interface point between PuddleStore and Raft.
*/

/*
	Creates an INode with the given parameters, stores it in the Tapestry network, and then
	creates an AGUID for it and maps that to the first version (in Tapestry) using Raft.
	Returns the new INode, its AGUID, and any error.
*/
func createInode(conn *zk.Conn, name string, class uint) (inode *INode, err error) {
	// param checks
	if strings.ContainsAny(name, INVALID_PATH_CHARS) {
		return nil, fmt.Errorf("invalid inode name (contains one of %s)", INVALID_PATH_CHARS)
	}
	if class != FILE && class != DIRECTORY {
		return nil, errors.New("inode must be either a FILE or DIRECTORY class")
	}

	// create the inode and store it's data in tapestry
	AGUID := newGUID()
	VGUID := newGUID()
	inode = &INode{conn, AGUID, VGUID, name, class, 0, make([]string, 0)}
	err = addObjectToTapestry(conn, inode, VGUID)
	if err != nil {
		return nil, err
	}

	// add a mapping from the node's AGUID to its (first) VGUID using raft
	err = setMappingInRaft(conn, AGUID, VGUID)
	if err != nil {
		return nil, err
	}
	return inode, nil
}

/*
	Updates the fields of the INOde with the given new parameters modified during a write,
	but only does so after writing and confirming the updated inode and mapping are stored
	in Tapestry and Raft.
*/
func (inode *INode) updateINodeVersionAfterWrite(newSize uint64, newBlocks []string) error {
	// create a new file object from the old that will be the new version,
	// generating a new VGUID that will signify this
	// (the eventually applying of these changes to the original file will mirror
	// the change in Raft of the mapping between AGUID and VGUID)
	newINode := &INode{
		conn:   inode.conn,
		AGUID:  inode.AGUID,
		VGUID:  newGUID(),
		Name:   inode.Name,
		Class:  inode.Class,
		Size:   newSize,
		Blocks: newBlocks,
	}

	// add the new version to Tapestry under the new VGUID,
	// then "atomically" update the mapping from the file's known AGUID to this new VGUID
	err := addObjectToTapestry(inode.conn, newINode, newINode.VGUID)
	if err != nil {
		// inode is left unmodified
		return fmt.Errorf("adding new inode to tapestry: %v", err)
	}

	err = setMappingInRaft(inode.conn, newINode.AGUID, newINode.VGUID)
	if err != nil {
		// inode is left unmodified
		return fmt.Errorf("updating inode mapping to raft: %v", err)
	}

	// apply changes to local version of inode
	inode.VGUID = newINode.VGUID
	inode.Size = newINode.Size
	inode.Blocks = newINode.Blocks
	return nil
}

/*
	Fetches and returns the object stored under aguid, expected to be a DBlock.
*/
func fetchDBlockByBGUID(conn *zk.Conn, bguid string) (dblock *DBlock, err error) {
	bytes, err := getObjectFromTapestry(conn, bguid)
	if err != nil {
		return nil, err
	}
	return decodeDBlock(bytes)
}

/*
	Fetches and returns the object stored under aguid, converted to be a INode. If the given INode was not found,
	(i.e the Raft did not have a key for it), this returns nil, nil. No error is thrown unless something actually
	went wrong.
*/
func fetchINodeByAGUID(conn *zk.Conn, aguid string) (inode *INode, err error) {
	bytes, err := fetchByAGUID(conn, aguid)
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		// KeyNotFoundException in Raft
		return nil, nil
	}
	return decodeInode(bytes, conn)
}

/*
   Given an AGUID known to represent an object in the system,
*/
func fetchByAGUID(conn *zk.Conn, aguid string) ([]byte, error) {
	// get raft node
	raftClient, err := zkGetRaftClient(conn)
	if err != nil || raftClient == nil {
		return nil, fmt.Errorf("fetching by aguid: %v", err)
	}

	// get current map from Raft servers
	rsp, err := raftClient.SendRequest(keyvaluestore.GET, []byte(aguid))
	if err != nil {
		return nil, fmt.Errorf("fetching VGUID from raft: %v", err)
	} else if rsp == nil {
		return nil, errors.New("fetching VGUID from raft: could not send request to Raft")
	}

	VGUID := rsp.Response
	Debug.Printf("VGUID %v returned for AGUID %v", VGUID, aguid)
	if VGUID == "" {
		// EQUIVALENT OF A KEYNOTFOUNDEXCEPTION
		return nil, nil
	}

	bytes, err := getObjectFromTapestry(conn, VGUID)
	if err != nil {
		return nil, fmt.Errorf("fetching by GUID from tapestry: %v", err)
	}
	return bytes, nil
}

/*
   	Finds the INode with key VGUID in tapestry and returns it. If no errors occur but the INode is not found,
	then it returns a nil pointer and nil error.
*/
func getObjectFromTapestry(conn *zk.Conn, GUID string) ([]byte, error) {

	// get Tapestry node
	tp, err := zkGetTapestryClient(conn)
	if err != nil || tp == nil {
		return nil, fmt.Errorf("retrieving from Tapestry: %v", err)
	}

	// fetch item
	bits, err := tp.Get(GUID)
	if err != nil {
		return nil, err
	}
	// because we can't store null values in tapestry, consider an empty byte array to be null
	if len(bits) == 0 {
		return nil, errors.New("object was erased")
	}
	return bits, nil
}

/*
	Adds the given Serializable object to the tapestry object store under the given GUID.
*/
func addObjectToTapestry(conn *zk.Conn, obj Serializable, GUID string) error {
	client, err := zkGetTapestryClient(conn)
	data, err := obj.encode()
	if err != nil || client == nil {
		return err
	}
	return client.Store(GUID, data)
}

/*
	Removes the given Serializable object under the given GUID from the tapestry object store.
*/
func removeObjectFromTapestry(conn *zk.Conn, GUID string) error {
	client, err := zkGetTapestryClient(conn)
	if err != nil || client == nil {
		return err
	}
	// remove = set data to empty (we can't send null values)
	return client.Store(GUID, []byte{})
}

/*
	Creates and come to a atomic consensus regarding the mapping between AGUID and VGUID
	using Raft. Returns any error
*/
func setMappingInRaft(conn *zk.Conn, AGUID, VGUID string) error {
	rClient, err := zkGetRaftClient(conn)
	if err != nil {
		return err
	}
	cmd := fmt.Sprintf("%v:%v", AGUID, VGUID)
	_, err = rClient.SendRequest(keyvaluestore.SET, []byte(cmd))
	return err
}

/*
	Creates and come to a atomic consensus regarding the mapping between AGUID and VGUID
	using Raft. Returns any error
*/
func deleteMappingInRaft(conn *zk.Conn, AGUID string) error {
	rClient, err := zkGetRaftClient(conn)
	if err != nil {
		return err
	}
	cmd := fmt.Sprintf("%v", AGUID)
	_, err = rClient.SendRequest(keyvaluestore.DELETE, []byte(cmd))
	return err
}
