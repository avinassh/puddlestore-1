package puddlestore

import (
	"bytes"
	"encoding/gob"
	"errors"

	"fmt"

	"github.com/gobuffalo/uuid"
	"github.com/samuel/go-zookeeper/zk"
)

/*
	The primary datatypes and methods of the PuddleStore file system.
*/

/************************************************************************************/
// File System Objects
/************************************************************************************/

// Concrete types of blocks
const (
	FILE = iota
	DIRECTORY
	DBLOCK
)

/*
	A high-level type describing the abstract notion of an object in a filesystem.
	This object can either be a Directory or a File Class, and can be used differently
	based on its Class.
*/
type INode struct {
	conn   *zk.Conn // pointer to Zookeeper connection; method of accessing global state in client library; not serialized
	AGUID  string   // unique identifier of this inode across versions; Raft maps between this and VGUID to designate versions
	VGUID  string   // unique identifier of this particular version of the inode; used to store it in Tapestry
	Name   string   // name of object
	Class  uint     // one of FILE or DIRECTORY
	Size   uint64   // either number of objects in directory or Size in bytes in file
	Blocks []string // either AGUIDs of children of a directory or BGUIDs of Data Blocks of a file
}

/*
	A block type to keep Data
*/
type DBlock struct {
	BGUID string
	Data  []byte
}

type Serializable interface {
	encode() ([]byte, error)
}

/************************************************************************************/
// INode Executors
/************************************************************************************/
/*
	Client connection and entry point into a PuddleStore filesystem.
	Opens a Zookeeper connection to interact with the filesystem and
	returns the INode corresponding to the root directory.
*/
func OpenConn(zkaddr string) (*INode, error) {
	conn, err := ZookeeperConnect(zkaddr)
	if err != nil {
		return nil, err
	}
	// don't allow opening conns to cluster that can't do anything
	ready, err := PuddleStoreReady(conn)
	if !ready {
		return nil, err
	}
	return ZookeeperGetRoot(conn)
}

/*
	Returns the root of the entire filesystem, using the internal Zookeeper connection.
*/
func (inode *INode) GetRoot() (*INode, error) {
	return ZookeeperGetRoot(inode.conn)
}

/*
	Open connects to PuddleStore and attempts to access the INode at the given path.
	It returns the INode object if one exists, and an error otherwise.
*/
func (inode *INode) Open(path string) (newInode *INode, err error) {
	return inode.OpenPath(NewPath(path))
}

/*
	Open connects to PuddleStore and attempts to access the INode at the given path.
	It returns the INode object if one exists, and an error otherwise.
*/
func (inode *INode) OpenPath(path *Path) (newInode *INode, err error) {
	curr, err := inode.GetRoot()
	if err != nil {
		return nil, err
	}

	navPath := path.List()
	for _, fName := range navPath {
		child, _, found, err := curr.GetChildByName(fName)
		if err != nil {
			return nil, err
		}
		if found {
			curr = child
		} else {
			return nil, fmt.Errorf("%s: no such file or directory", path.String())
		}
	}
	return curr, nil
}

/*
	Remove attempts to remove the inode from the file system.
	If the INode is a Directory, all children inodes (files and directories) are removed.
*/
func (inode *INode) Remove(parentDir *Directory) (err error) {
	if inode.IsDirectory() {
		return inode.AsDirectory().Remove(parentDir)
	} else {
		return inode.AsFile().Remove(parentDir)
	}
}

/*
	Closes the tapestry connection associated with the given inode.
	Renders the PuddleStore inaccessible from this session
*/
func (inode *INode) CloseConn() {
	inode.conn.Close()
}

/*
	Reload gets a fresh copy of the given INode using its AGUID. Can return
	nil, nil if the node does not exist any longer.
*/
func (inode *INode) Reload() (*INode, error) {
	return fetchINodeByAGUID(inode.conn, inode.AGUID)
}

/************************************************************************************/
// INode Helpers
/************************************************************************************/
func (inode *INode) GetAGUID() string {
	return inode.AGUID
}

func (inode *INode) GetVGUID() string {
	return inode.VGUID
}

func (inode *INode) GetName() string {
	return inode.Name
}

func (inode *INode) GetSize() uint64 {
	return inode.Size
}

func (inode *INode) IsFile() bool {
	return inode.Class == FILE
}

func (inode *INode) IsDirectory() bool {
	return inode.Class == DIRECTORY
}

func (inode *INode) AsFile() *File {
	if inode.IsFile() {
		return &File{*inode}
	}
	return nil
}

func (inode *INode) AsDirectory() *Directory {
	if inode.IsDirectory() {
		return &Directory{*inode}
	}
	return nil
}

func (inode *INode) Clone() *INode {
	clone := INode{
		inode.conn,
		inode.AGUID,
		inode.VGUID,
		inode.Name,
		inode.Class,
		inode.Size,
		make([]string, len(inode.Blocks)),
	}
	copy(clone.Blocks, inode.Blocks)
	return &clone
}

/*
	Block list helpers
*/

// Returns INode corresponding to item at position int.
// Can return nil, nil!
func (inode *INode) GetChildAt(pos int) (*INode, error) {
	if !inode.IsDirectory() {
		return nil, errors.New("inode is not a directory")
	}
	AGUID := inode.Blocks[pos]
	return fetchINodeByAGUID(inode.conn, AGUID)
}

// Returns INode with the given name if it exists, and nil if it's not found. Returns an error only if there is a connection failure.
func (inode *INode) GetChildByName(name string) (in *INode, index uint64, found bool, err error) {
	for i, aguid := range inode.Blocks {
		in, err := fetchINodeByAGUID(inode.conn, aguid)
		if err != nil {
			return nil, uint64(i), false, err
		}
		if in != nil && in.GetName() == name {
			return in, uint64(i), true, nil
		}
	}
	return nil, 0, false, nil
}

// Returns a DBlock corresponding to the item at position int
func (inode *INode) getDBlockAt(pos uint64) (*DBlock, error) {
	if !inode.IsFile() {
		return nil, errors.New("inode is not a file")
	}
	BGUID := inode.Blocks[pos]
	db, err := fetchDBlockByBGUID(inode.conn, BGUID)
	if err != nil {
		return nil, err
	}
	return db, nil
}

/*
	Adds the given block AGUID to the list of blocks of this inode
*/
func (inode *INode) addBlock(aguid string) {
	inode.Blocks = append(inode.Blocks, aguid)
}

/*
	Removes the given block AGUID from the list of blocks of this inode
*/
func (inode *INode) removeBlock(aguid string) (removed bool) {
	contains, index := inode.ContainsBlock(aguid)
	if contains {
		inode.Blocks = append(inode.Blocks[:index], inode.Blocks[index+1:]...)
	}
	return contains
}

/*
	Returns a boolean and the index of the aguid if it is found
*/
func (inode *INode) ContainsBlock(aguid string) (contains bool, index int) {
	for index, aguidHere := range inode.Blocks {
		if aguidHere == aguid {
			return true, index
		}
	}
	return false, 0
}

/*
	Serialize an INode
*/
func (inode *INode) encode() ([]byte, error) {
	// TODO: testing
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	err := enc.Encode(inode)
	if err != nil {
		return nil, err
	}
	return data.Bytes(), nil
}

/*
	Deserialize an INode
*/
func decodeInode(input []byte, conn *zk.Conn) (*INode, error) {
	// TODO: test this
	var data bytes.Buffer
	var output INode
	data.Write(input)

	dec := gob.NewDecoder(&data)
	err := dec.Decode(&output)
	if err != nil {
		return nil, err
	}
	// make sure to set connection to zookeeper to have a continuous reference
	output.conn = conn

	// validate the contents as an internal check
	if err = output.validate(-1); err != nil {
		return nil, err
	}
	return &output, nil
}

/*
	Ensures the internal fields of the inode are consistent.
	The expectedClass field is used to confirm the Class of the INode
	if non-negative (set to negative 1 if don't care).
*/
func (inode *INode) validate(expectedClass int) error {
	if !inode.IsFile() && !inode.IsDirectory() {
		return errors.New("INode Class was incorrect")
	}
	if expectedClass > 0 && inode.Class != uint(expectedClass) {
		return fmt.Errorf("INode was not of expected Class (was %v, expected %v)", inode.Class, expectedClass)
	}
	if inode.AGUID == "" {
		return errors.New("INode AGUID field was empty")
	}
	if inode.VGUID == "" {
		return errors.New("INode VGUID field was empty")
	}
	if inode.Name == "" {
		return errors.New("INode Name field was empty")
	}
	if inode.IsFile() && inode.Size/DBLOCKSIZE > uint64(len(inode.Blocks)) {
		return fmt.Errorf("INode File Size field was inconsistent with Blocks (Size: %d, actual: %d)", inode.Size, len(inode.Blocks))
	}
	if inode.IsDirectory() && inode.Size != uint64(len(inode.Blocks)) {
		return fmt.Errorf("INode Directory Size field was inconsistent with Blocks (Size: %d, actual: %d)", inode.Size, len(inode.Blocks))
	}
	return nil
}

/************************************************************************************/
// DBlock Methods
/************************************************************************************/

// Constructs a DBlock with no Data and a BGUID
func newDBlock() *DBlock {
	var db DBlock
	db.BGUID = newGUID()
	db.Data = make([]byte, DBLOCKSIZE)
	return &db
}

/*
	Serialize a dBlock
*/
func (db *DBlock) encode() ([]byte, error) {
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	err := enc.Encode(db)
	return data.Bytes(), err
}

func decodeDBlock(input []byte) (*DBlock, error) {
	var data bytes.Buffer
	data.Write(input)
	dec := gob.NewDecoder(&data)
	var db DBlock
	err := dec.Decode(&db)
	return &db, err
}

/*
	Returns BGUID for a DBlock. Sets a random BGUID if not already assigned.
*/
func (db *DBlock) getBGUID() string {
	if db.BGUID == "" {
		db.BGUID = newGUID()
		return db.BGUID
	}
	return db.BGUID
}

/*
	Fills the given DBlock with Data from buf from pos (this will *overwrite* the data at pos)
	Returns the number of bytes written!
*/
func (db *DBlock) Fill(start uint64, buf *bytes.Buffer) uint64 {
	if start >= uint64(len(db.Data)) {
		return 0
	}
	maxN := int(DBLOCKSIZE - start)
	toWrite := buf.Next(maxN)
	// if we're not filling up the whole buffer, make sure to add the parts back
	// that are past what we're writing
	if len(toWrite) < maxN {
		db.Data = append(db.Data[:start], append(toWrite, db.Data[int(start)+len(toWrite):]...)...)
	} else {
		db.Data = append(db.Data[:start], toWrite...)
	}
	return uint64(len(toWrite))
}

/*
	Returns a clone of the given DBlock, with a new BGUID
*/
func (db *DBlock) Clone() *DBlock {
	clone := newDBlock()
	var buf bytes.Buffer
	buf.Write(db.Data)
	clone.Data = buf.Bytes()
	return clone
}

func (db *DBlock) Delete(parent *File) error {
	found := parent.removeBlock(db.getBGUID())
	if !found {
		return errors.New("delete: not valid child of parent")
	}

	// remove block from tapestry -- there is no raft mapping
	return removeObjectFromTapestry(parent.conn, db.getBGUID())
}

/************************************************************************************/
// General Helpers
/************************************************************************************/

/*
	Returns a new unique identifier for use as a BGUID in INodes and DBlocks.
*/
func newGUID() string {
	u, _ := uuid.NewV4()
	return u.String()
}
