package puddlestore

import (
	"errors"
	"fmt"
)

/*
	Methods related to directory creation, deletion and population in PuddleStore.
*/

/*
	A wrapper Directory type to hold any amount of other Inodes
*/
type Directory struct {
	INode
}

// to implement Serializable
func (dir *Directory) encode() ([]byte, error) {
	return dir.INode.encode()
}

/************************************************************************************/
// Directory Methods
/************************************************************************************/
/*
	Contents attempts to list all the Inodes in the Directory, in an arbitrary order.
*/
func (dir *Directory) Contents() (inodes []*INode, err error) {
	inodes = make([]*INode, 0)
	for _, aguid := range dir.Blocks {
		inode, err := fetchINodeByAGUID(dir.conn, aguid)
		if err != nil {
			return nil, err
		}
		if inode != nil {
			// inode might be nil if this item has been deleted!
			inodes = append(inodes, inode)
		}
	}
	return inodes, nil
}

/*
	CreateFile attempts to create and initialize an empty file in PuddleStore in the given directory.
*/
func (dir *Directory) CreateFile(name string) (newFile *File, err error) {
	inode, err := createInode(dir.conn, name, FILE)
	if err != nil {
		return nil, err
	}
	dir.updateINodeVersionAfterWrite(dir.Size+1, append(dir.Blocks, inode.AGUID))
	return inode.AsFile(), err
}

/*
	CreateDirectory attempts to create and initialize an empty directory in PuddleStore in the given directory.
*/
func (dir *Directory) CreateDirectory(name string) (newDir *Directory, err error) {
	inode, err := createInode(dir.conn, name, DIRECTORY)
	if err != nil {
		return nil, err
	}
	dir.updateINodeVersionAfterWrite(dir.Size+1, append(dir.Blocks, inode.AGUID))
	return inode.AsDirectory(), err
}

/*
	Removes the directory and all its children.
*/
func (dir *Directory) Remove(parentDir *Directory) error {
	return dir.remove(parentDir, true)
}

/*
	Helper for Remove.
	If the updateParent is false, the function doesn't bother to update the parent
*/
func (dir *Directory) remove(parentDir *Directory, updateParent bool) (err error) {
	// clone parent to make our changes, which will be applied only after everything else
	newParent := parentDir.Clone()

	if updateParent {
		found := newParent.removeBlock(dir.AGUID)
		if !found {
			return errors.New("directory not found in parent directory")
		} else {
			newParent.Size--
		}
	}

	// remove child inodes using their remove methods (recursive)
	for _, aguid := range dir.Blocks {
		node, err2 := fetchINodeByAGUID(dir.conn, aguid)
		if err2 != nil {
			// don't stop deletion process on error from subdir
			err = err2
			fmt.Errorf("WARNING: subinode of directory could not be removed: %v", err2)
		} else if node != nil {
			// may nil if it has been deleted
			if node.IsFile() {
				node.AsFile().remove(dir, false)
			} else if node.IsDirectory() {
				node.AsDirectory().remove(dir, false)
			}
		}
	}

	// remove this directory's AGUID->VGUID mapping from raft
	err = deleteMappingInRaft(dir.conn, dir.AGUID)
	if err != nil {
		return err
	}
	// remove from tapestry using VGUID
	err = removeObjectFromTapestry(dir.conn, dir.GetVGUID())
	// don't let error effect execution, but return it

	// update parentDir in our filesystem
	if updateParent {
		err = parentDir.updateINodeVersionAfterWrite(newParent.Size, newParent.Blocks)
	}
	return err
}
