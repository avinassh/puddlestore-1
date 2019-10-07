package puddlestore

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"
)

/*
	Methods related to files and Data storage in PuddleStore.
*/

/*
	A wrapper File type to contain Data stored in the filesystem.
*/
type File struct {
	INode
}

// to implement Serializable
func (file File) encode() ([]byte, error) {
	return file.INode.encode()
}

/************************************************************************************/
// File Methods
/************************************************************************************/

/*
	Write attempts to replace the current data in file with that in data, extending the Size of the file if necessary.
	Note that Write will truncate any part of the file larger than the given data, such that the new file will only contain data.
	It does this through copy-on-write, so the current file block will be modified (with a new ID, new size, and new block list),
	and any of its data blocks that are being modified will be replicated, re-published, and updated in the file's
	block list. Any data blocks that weren't modified will not change (besides those being truncated)
*/
func (file *File) Write(data []byte) error {
	return file.write(0, data, true)
}

/*
	WriteTo is similar to Write but it writes data starting at start position in the file, and WILL NOT truncate
	the file at the end
*/
func (file *File) WriteTo(start uint64, data []byte) error {
	return file.write(start, data, false)
}

func (file *File) write(start uint64, data []byte, truncate bool) error {

	// if there is no data, do nothing
	if len(data) == 0 {
		return nil
	}

	var buf bytes.Buffer
	// if the start is greater than the file size, append 0 bytes between
	// the current end of the file and the data to be written, then reset
	// start to be the end of the file so it writes normally.
	// (we append a number of bytes equal to the difference between start
	//  and the previous end of the file)
	if start > file.Size {
		for i := file.Size; i < start; i++ {
			buf.WriteByte(0)
		}
		buf.Write(data)
		start = file.Size
	} else {
		// otherwise, simply fill the buffer with our data and start at start
		buf.Write(data)
	}

	// work with local copies of file metadata, as we'll do an "atomic" write at the end
	newBlocks := make([]string, len(file.Blocks))
	copy(newBlocks, file.Blocks)

	// flush buffer into data blocks and write them
	curWritePos := start
	for index := curWritePos / DBLOCKSIZE; buf.Len() > 0; index++ {
		if index >= uint64(len(file.Blocks)) {
			// create new block
			db := newDBlock()
			written := db.Fill(0, &buf)
			curWritePos += written

			// add data to Tapestry under BGUID (note there's no Raft mapping because
			// these blocks are always copied on write)
			err := addObjectToTapestry(file.conn, db, db.BGUID)
			if err != nil {
				return fmt.Errorf("adding data block to tapestry: %v", err)
			}

			// add this new block at the end of the block list
			newBlocks = append(newBlocks, db.BGUID)

		} else {
			// gets current block
			db, err := file.getDBlockAt(index)
			if err != nil {
				return fmt.Errorf("retrieving block: %v", err)
			}

			// clone block and update (it will have a new BGUID because it has new data),
			clone := db.Clone()
			clone.BGUID = newGUID()
			newBlocks[index] = clone.BGUID // make sure to update it in the new block list

			// fill the cloned block with the new data that it should have out of our writing buffer
			// (index to start writing from is relative to top of previous block)
			written := clone.Fill(curWritePos%DBLOCKSIZE, &buf)
			curWritePos += written

			// add data to Tapestry under new BGUID (note there's no Raft mapping because
			// these blocks are always copied on write)
			err = addObjectToTapestry(file.conn, clone, clone.BGUID)
			if err != nil {
				return fmt.Errorf("adding data block to tapestry: %v", err)
			}
		}
	}

	newSize := max(file.Size, curWritePos) // assuming we're not truncating
	// if we're truncating, we need to clean up any blocks that don't contain data in the file anymore
	// (it's fine to leave some data in blocks that are partially still used in the file)
	if truncate {
		newSize = curWritePos
		// if there is extra space past curWritePos that is greater than a block size, we'll
		// have extra blocks, so remove those
		lastUsedBlockIndex := int(curWritePos) / DBLOCKSIZE
		if len(newBlocks) > lastUsedBlockIndex+1 {
			// don't remove those blocks from tapestry in case something goes wrong,
			// just remove them from reference list
			newBlocks = newBlocks[:lastUsedBlockIndex+1]
		}
	}
	// actually apply the updated fields by updating the File struct,
	// re-adding it to Tapestry, and adding the new mapping to Raft.
	return file.updateINodeVersionAfterWrite(newSize, newBlocks)
}

/*
	Read attempts to read the entire contents of the file.
*/
func (file *File) Read() ([]byte, error) {
	return file.ReadN(0, 0)
}

/*
	ReadToEnd attempts to reads all bytes between start and then end of the file.
*/
func (file *File) ReadFrom(start uint64) ([]byte, error) {
	return file.ReadN(start, 0)
}

/*
	ReadN attempts to read up to size bytes of data staring at position pos.
	If size is 0, all bytes until the end of the file will be returned.
*/
func (file *File) ReadN(start uint64, size uint64) ([]byte, error) {
	// read all the bytes and write them to the buffer
	bts := bytes.NewBuffer(make([]byte, 0))
	if size == 0 {
		size = file.Size
	}
	numLeft := min(size, file.Size)

	// we deal with the block to start in in the for loop, but we also
	// need to deal with the offset within the first block
	startInBlock := start % DBLOCKSIZE

	// iterate over the BLOCKS in the inode, reading as much as needed from each
	// (starting from the first block that i is in, via truncation)
	for i := start / DBLOCKSIZE; i < uint64(len(file.Blocks)); i++ {
		block, err := file.getDBlockAt(i)
		if err != nil {
			return bts.Bytes(), fmt.Errorf("retrieving data block: %v", err)
		}
		readable := DBLOCKSIZE - startInBlock
		if numLeft <= readable {
			// read partial Data block and terminate
			bts.Write(block.Data[startInBlock : startInBlock+numLeft])
			break
		} else {
			// read entire Data block
			bts.Write(block.Data[startInBlock:])
			numLeft -= readable
		}

		// because we only care about the offset in the first block, we can always reset
		// the start in block pos after the first iteration
		startInBlock = 0
	}

	// return contents of buffer
	return bts.Bytes(), nil
}

/*
	Removes the file and all its children from parentDir
*/
func (file *File) Remove(parentDir *Directory) error {
	return file.remove(parentDir, true)
}

/*
	Helper for remove. If the parent directory is to be updated with our changes,
	updateParent should be true, and if not then it is left false.
*/
func (file *File) remove(parentDir *Directory, updateParent bool) error {
	// clone parent to make our changes, which will be applied only after everything else
	newParent := parentDir.Clone()

	if updateParent {
		found := newParent.removeBlock(file.AGUID)
		if !found {
			return errors.New("file not found in parent directory")
		} else {
			newParent.Size--
		}
	}

	// remove all the blocks of the file
	for _, bguid := range file.Blocks {
		removeObjectFromTapestry(file.conn, bguid)
	}

	// remove AGUID->VGUD mapping from raft
	err := deleteMappingInRaft(file.conn, file.AGUID)
	if err != nil {
		return err
	}
	// zero out data stored under VGUID in Tapestry
	// (not really necessary but good to do)
	err = removeObjectFromTapestry(file.conn, file.GetVGUID())
	// don't let error effect execution, but return it

	// update parentDir in our filesystem
	if updateParent {
		err = parentDir.updateINodeVersionAfterWrite(newParent.Size, newParent.Blocks)
	}
	return err
}

func min(num1 uint64, num2 uint64) (res uint64) {
	if num1 <= num2 {
		return num1
	} else {
		return num2
	}
}

func max(num1 uint64, num2 uint64) (res uint64) {
	if num1 >= num2 {
		return num1
	} else {
		return num2
	}
}
