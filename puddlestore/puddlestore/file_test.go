package puddlestore

import (
	"testing"

	"math/rand"

	"bytes"

	"github.com/stretchr/testify/assert"
)

var fileData []byte

func WriteAndCheck(t *testing.T, file *File, bts []byte) {
	if len(bts) > 0 {
		assert.Equal(t, uint64(len(fileData)), file.Size)
		err := file.Write(bts)
		assert.NoError(t, err)
		// update our version of the data
		assert.Equal(t, len(bts), int(file.Size))
		fileData = bts
		ReadAndCheck(t, file, 0, 0)
	}
}

func WriteToAndCheck(t *testing.T, file *File, start uint64, bts []byte) {
	if len(bts) > 0 {
		prevLen := file.Size
		assert.Equal(t, uint64(len(fileData)), prevLen)
		err := file.WriteTo(start, bts)
		assert.NoError(t, err)
		// update our version of the data
		var newFileData []byte
		if len(fileData) < int(start) {
			// if start is past end of file, add zeros inbetween end of file and start
			inBetween := make([]byte, int(start)-len(fileData))
			newFileData = append(fileData, inBetween...)
			newFileData = append(newFileData, bts...)
		} else if int(start)+len(bts) < len(fileData) {
			// if the file is larger than start, but the inserted data doesn't take up the whole rest of the file,
			// insert it in the middle
			newFileData = append(fileData[:start], append(bts, fileData[int(start)+len(bts):]...)...)
		} else {
			// otherwise we're writing past the end so just add bts
			newFileData = append(fileData[:start], bts...)
		}
		assert.Equal(t, len(newFileData), int(file.Size),
			"start: %d, len(bts): %d, len(newFileData): %d, len(fileData): %d, prevLen: %d",
			start, len(bts), len(newFileData), len(fileData), prevLen)
		fileData = newFileData
		ReadAndCheck(t, file, start, uint64(len(bts)))
	}
}

func ReadAndCheck(t *testing.T, file *File, start uint64, size uint64) {
	if start < uint64(len(file.Blocks)) {
		data, err := file.ReadN(start, size)
		assert.NoError(t, err)
		assert.NotNil(t, data)
		if err == nil && data != nil {
			if size == 0 {
				assert.Len(t, data, int(file.Size))
				assert.Equal(t, data, fileData)
			} else {
				assert.True(t, int(size) >= len(data), "size: %d, lendata: %d", size, len(data))
				if len(fileData)-int(start) > int(size) {
					assert.Equal(t, fileData[start:start+size], data,
						"start: %d, size: %d, len(data): %d, len(fileData): %d",
						start, size, len(data), len(fileData))
				}
			}
		}
	}
}

const NUM_READWRITES = 200
const MAX_FILE_SIZE = DBLOCKSIZE * 4
const MAX_WRITE_SIZE = DBLOCKSIZE * 3

func TestWriteToIndex(t *testing.T) {
	_, _, err := StartPuddleStoreCluster(DEFAULT_ZK_ADDR, 10)
	assert.NoError(t, err)
	conn, err := ZookeeperConnect(DEFAULT_ZK_ADDR)
	assert.NoError(t, err)
	if conn == nil {
		return
	}

	fileData = make([]byte, 0)
	inode, err := createInode(conn, "test", FILE)
	assert.NoError(t, err)
	assert.NotNil(t, inode)
	if inode == nil {
		return
	}
	file := inode.AsFile()

	t.Run("random write tos", func(t *testing.T) {
		for i := 0; i < NUM_READWRITES; i++ {
			pos := uint64(rand.Intn(MAX_FILE_SIZE))
			size := rand.Intn(MAX_WRITE_SIZE)
			WriteToAndCheck(t, file, pos, GetRandBytes(size))
		}
	})

	t.Run("random writes", func(t *testing.T) {
		for i := 0; i < NUM_READWRITES; i++ {
			size := rand.Intn(MAX_WRITE_SIZE)
			WriteAndCheck(t, file, GetRandBytes(size))
		}
	})

	t.Run("random reads", func(t *testing.T) {
		for i := 0; i < NUM_READWRITES; i++ {
			pos := uint64(rand.Intn(MAX_FILE_SIZE))
			size := uint64(rand.Intn(MAX_WRITE_SIZE))
			ReadAndCheck(t, file, pos, size)
		}
	})
}

func TestFill(t *testing.T) {
	db := newDBlock()
	assert.NotNil(t, db)
	if db == nil {
		return
	}
	assert.NotNil(t, db.Data)
	if db.Data == nil {
		return
	}
	assert.Len(t, db.Data, DBLOCKSIZE)

	t.Run("fill nothing", func(t *testing.T) {
		data := make([]byte, 0)
		numWritten := db.Fill(0, bytes.NewBuffer(data))
		assert.Equal(t, uint64(0), numWritten)
		assert.Len(t, db.Data, DBLOCKSIZE)
	})

	t.Run("fill less than DBLOCKSIZE", func(t *testing.T) {
		data := GetRandBytes(DBLOCKSIZE / 2)
		assert.Len(t, data, DBLOCKSIZE/2)
		numWritten := db.Fill(0, bytes.NewBuffer(data))
		assert.Equal(t, uint64(DBLOCKSIZE/2), numWritten)
		assert.Len(t, db.Data, DBLOCKSIZE)
		assert.Equal(t, data, db.Data[:DBLOCKSIZE/2])
	})

	t.Run("fill DBLOCKSIZE", func(t *testing.T) {
		data := GetRandBytes(DBLOCKSIZE)
		assert.Len(t, data, DBLOCKSIZE)
		numWritten := db.Fill(0, bytes.NewBuffer(data))
		assert.Equal(t, uint64(DBLOCKSIZE), numWritten)
		assert.Len(t, db.Data, DBLOCKSIZE)
		assert.Equal(t, data, db.Data)
	})

	t.Run("fill more than DBLOCKSIZE", func(t *testing.T) {
		data := GetRandBytes(DBLOCKSIZE * 2)
		assert.Len(t, data, DBLOCKSIZE*2)
		numWritten := db.Fill(0, bytes.NewBuffer(data))
		assert.Equal(t, uint64(DBLOCKSIZE), numWritten)
		assert.Len(t, db.Data, DBLOCKSIZE)
		assert.Equal(t, data[:DBLOCKSIZE], db.Data)
	})

	t.Run("fill less than BLOCKSIZE at offset", func(t *testing.T) {
		data := GetRandBytes(DBLOCKSIZE / 4)
		assert.Len(t, data, DBLOCKSIZE/4)
		numWritten := db.Fill(DBLOCKSIZE/2, bytes.NewBuffer(data))
		assert.Equal(t, uint64(DBLOCKSIZE/4), numWritten)
		assert.Len(t, db.Data, DBLOCKSIZE)
		assert.Equal(t, data[:DBLOCKSIZE/4], db.Data[DBLOCKSIZE/2:3*DBLOCKSIZE/4])
	})

	t.Run("fill DBLOCKSIZE at offset", func(t *testing.T) {
		data := GetRandBytes(DBLOCKSIZE / 2)
		assert.Len(t, data, DBLOCKSIZE/2)
		numWritten := db.Fill(DBLOCKSIZE/2, bytes.NewBuffer(data))
		assert.Equal(t, uint64(DBLOCKSIZE/2), numWritten)
		assert.Len(t, db.Data, DBLOCKSIZE)
		assert.Equal(t, data, db.Data[DBLOCKSIZE/2:])
	})

	t.Run("fill more than DBLOCKSIZE at offset", func(t *testing.T) {
		data := GetRandBytes(DBLOCKSIZE)
		assert.Len(t, data, DBLOCKSIZE)
		numWritten := db.Fill(DBLOCKSIZE/2, bytes.NewBuffer(data))
		assert.Equal(t, uint64(DBLOCKSIZE/2), numWritten)
		assert.Len(t, db.Data, DBLOCKSIZE)
		assert.Equal(t, data[:DBLOCKSIZE/2], db.Data[DBLOCKSIZE/2:])
	})
}
