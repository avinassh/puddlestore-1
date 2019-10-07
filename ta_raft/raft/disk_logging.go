package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
)

// FileData represents a file to which a Raft node is writing its logs.
type FileData struct {
	// Active file descriptor of to file
	fd *os.File

	// Size of file after reading it in and after writes
	size int64

	// Filename of file
	filename string

	// Map from LogEntry index to size of file before that index starts; used only
	// for the file storing the LogCache.
	idxMap map[uint64]int64

	// Is the fd open or not?
	open bool
}

////////////////////////////////////////////////////////////////////////////////
// File I/O functions for StableState                                         //
////////////////////////////////////////////////////////////////////////////////

func openStableStateForWrite(fileData *FileData) error {
	if fileExists(fileData.filename) {
		fd, err := os.OpenFile(fileData.filename, os.O_APPEND|os.O_WRONLY, 0600)
		fileData.fd = fd
		fileData.open = true
		return err
	}

	return errors.New("StableState file does not exist")
}

// CreateStableState opens the stable state log file with the right permissions.
func CreateStableState(fileData *FileData) error {
	fd, err := os.OpenFile(fileData.filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	fileData.fd = fd
	fileData.open = true
	return err
}

// ReadStableState reads the stable store log file (or the backup if that fails),
// and returns the decoded stableState.
func ReadStableState(fileData *FileData) (*StableState, error) {
	f, err := os.Open(fileData.filename)
	if err != nil {
		Error.Printf("unable to open stable store file: %v\n", err)
	}

	stat, err := f.Stat()
	if err != nil {
		Error.Printf("unable to stat stable store file: %v\n", err)
		f.Close()
		return nil, err
	}

	ss, err := readStableStateEntry(f, int(stat.Size()))
	f.Close()

	if err != nil {
		// For some reason we failed to read our stable state file, try backup file.
		backupFilename := fmt.Sprintf("%v.bak", fileData.filename)
		backupFile, err := os.Open(backupFilename)
		if err != nil {
			Error.Printf("unable to read stable store backup file: %v\n", err)
			return nil, err
		}

		stat, err := f.Stat()
		if err != nil {
			Error.Printf("unable to stat stable store backup file: %v\n", err)
			backupFile.Close()
			return nil, err
		}

		ss, err = readStableStateEntry(f, int(stat.Size()))
		if err != nil {
			Error.Printf("unable to read stable storage or its backup: %v\n", err)
			backupFile.Close()
			return nil, err
		}

		backupFile.Close()

		// Successfully read backup file, copy it and make it the live log
		err = os.Remove(fileData.filename)
		if err != nil {
			return nil, err
		}

		err = copyFile(backupFilename, fileData.filename)
		if err != nil {
			return nil, err
		}

		return ss, nil
	}

	return ss, nil
}

// WriteStableState writes the current node's stable state to a log file.
func WriteStableState(fileData *FileData, ss StableState) error {
	// Backup old stable state
	backupFilename := fmt.Sprintf("%v.bak", fileData.filename)
	err := backupStableState(fileData, backupFilename)
	if err != nil {
		return fmt.Errorf("Backup failed: %v", err)
	}

	// Windows does not allow truncation of open file, must close first
	fileData.fd.Close()

	// Truncate live stable state i.e. empty it out
	err = os.Truncate(fileData.filename, 0)
	if err != nil {
		return fmt.Errorf("Truncation failed: %v", err)
	}

	// Open the log file
	fd, err := os.OpenFile(fileData.filename, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("unable to open stable state file: %v", err)
	}

	fileData.fd = fd

	// Write out stable state to live version
	bytes, err := getStableStateBytes(ss)
	if err != nil {
		return err
	}

	numBytes, err := fileData.fd.Write(bytes)
	if err != nil {
		return fmt.Errorf("unable to write to stable state file: %v", err)
	}

	if numBytes != len(bytes) {
		panic("did not write correct amount of bytes for some reason for ss")
	}

	err = fileData.fd.Sync()
	if err != nil {
		return fmt.Errorf("Sync #2 failed: %v", err)
	}

	// Remove backup file
	err = os.Remove(backupFilename)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("Remove failed: %v", err)
	}

	return nil
}

// backupStableState makes a backup copy of the given stable state log file
func backupStableState(fileData *FileData, backupFilename string) error {
	if fileData.open && fileData.fd != nil {
		err := fileData.fd.Close()
		fileData.open = false
		if err != nil {
			return fmt.Errorf("Closing file failed: %v", err)
		}
	}

	err := os.Remove(backupFilename)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("Remove failed: %v", err)
	}

	err = copyFile(fileData.filename, backupFilename)
	if err != nil {
		return fmt.Errorf("File copy failed: %v", err)
	}

	err = openStableStateForWrite(fileData)
	if err != nil {
		return fmt.Errorf("Opening stable state for writing failed: %v", err)
	}

	return nil
}

func getStableStateBytes(ss StableState) ([]byte, error) {
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	err := e.Encode(ss)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func readStableStateEntry(f *os.File, size int) (*StableState, error) {
	b := make([]byte, size)
	leSize, err := f.Read(b)
	if err != nil {
		return nil, err
	}
	if leSize != size {
		panic("The stable state log may be corrupt, cannot proceed")
	}

	buff := bytes.NewBuffer(b)
	var ss StableState
	dataDecoder := gob.NewDecoder(buff)
	err = dataDecoder.Decode(&ss)
	if err != nil {
		return nil, err
	}

	return &ss, nil
}

////////////////////////////////////////////////////////////////////////////////
// File I/O functions for LogCache                                            //
////////////////////////////////////////////////////////////////////////////////

func openRaftLogForWrite(fileData *FileData) error {
	if fileExists(fileData.filename) {
		fd, err := os.OpenFile(fileData.filename, os.O_APPEND|os.O_WRONLY, 0600)
		fileData.fd = fd
		fileData.open = true
		return err
	}

	return errors.New("Raftfile does not exist")
}

// CreateRaftLog opens the log cache log file with the right permissions.
func CreateRaftLog(fileData *FileData) error {
	fd, err := os.OpenFile(fileData.filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	fileData.fd = fd
	fileData.size = int64(0)
	fileData.idxMap = make(map[uint64]int64)
	fileData.open = true
	return err
}

// ReadRaftLog reads the log cache log file (or the backup if that fails),
// and populates the decoded LogCache.
func ReadRaftLog(fileData *FileData) (LogCache, error) {
	f, err := os.Open(fileData.filename)
	defer f.Close()
	fileData.idxMap = make(map[uint64]int64)

	entries := make(LogCache, 0)

	fileLocation := int64(0)
	for err != io.EOF {
		size, err := readStructSize(f)
		if err != nil {
			if err == io.EOF {
				break
			}
			Error.Printf("Error reading struct size: %v at loc: %v\n", err, fileLocation)
			fileData.open = false
			return entries, err
		}

		entry, err := readLogEntry(f, size)
		if err != nil {
			Error.Printf("Error reading log entry: %v at loc: %v\n", err, fileLocation)
			fileData.open = false
			return entries, err
		}
		fileData.idxMap[entry.Index] = fileLocation
		fileLocation += INT_SIZE_BYTES + int64(size)
		entries = append(entries, *entry)
	}

	fileData.open = false
	return entries, nil
}

// AppendLogEntry writes the given log entry to the end of the log cache log file.
func AppendLogEntry(fileData *FileData, entry *LogEntry) error {
	sizeIdx := fileData.size

	logBytes, err := getLogEntryBytes(entry)
	if err != nil {
		return err
	}
	size, err := getSizeBytes(len(logBytes))
	if err != nil {
		return err
	}

	numBytesWritten, err := fileData.fd.Write(size)
	if err != nil {
		return err
	}

	if int64(numBytesWritten) != INT_SIZE_BYTES {
		panic("int gob size is not correct, cannot proceed")
	}

	fileData.size += int64(numBytesWritten)

	err = fileData.fd.Sync()
	if err != nil {
		return err
	}

	numBytesWritten, err = fileData.fd.Write(logBytes)
	if err != nil {
		return err
	}

	if numBytesWritten != len(logBytes) {
		panic("did not write correct amount of bytes for some reason for log entry")
	}

	fileData.size += int64(numBytesWritten)

	err = fileData.fd.Sync()
	if err != nil {
		return err
	}

	// Update index mapping for this entry
	fileData.idxMap[entry.Index] = int64(sizeIdx)

	return nil
}

// TruncateLog removes the log entries at and after index from the log cache log file.
func TruncateLog(raftLogFd *FileData, index uint64) error {
	newFileSize, exist := raftLogFd.idxMap[index]
	if !exist {
		return fmt.Errorf("Truncation failed, log index %v doesn't exist\n", index)
	}

	// Windows does not allow truncation of open file, must close first
	raftLogFd.fd.Close()

	err := os.Truncate(raftLogFd.filename, newFileSize)
	if err != nil {
		return err
	}

	fd, err := os.OpenFile(raftLogFd.filename, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("unable to open log cache log file: %v", err)
	}

	raftLogFd.fd = fd

	for i := index; i < uint64(len(raftLogFd.idxMap)); i++ {
		delete(raftLogFd.idxMap, i)
	}
	raftLogFd.size = newFileSize

	return nil
}

const INT_SIZE_BYTES int64 = 5

func getSizeBytes(size int) ([]byte, error) {
	b := make([]byte, INT_SIZE_BYTES)

	// The returned value of the number of bytes written may be less than
	// INT_SIZE_BYTES but that's okay, they will be zero padded.
	_ = binary.PutUvarint(b, uint64(size))

	return b, nil
}

func decodeSizeBytes(b []byte) (int, error) {
	if int64(len(b)) != INT_SIZE_BYTES {
		panic("length of size bytes are not the correct size")
	}
	buf := bytes.NewBuffer(b)
	size, err := binary.ReadUvarint(buf)
	if err != nil {
		return -1, err
	}
	return int(size), nil
}

func getLogEntryBytes(entry *LogEntry) ([]byte, error) {
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	err := e.Encode(*entry)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func readStructSize(f *os.File) (int, error) {
	// Read bytes for size value
	b := make([]byte, INT_SIZE_BYTES)
	sizeBytes, err := f.Read(b)

	if err != nil {
		return -1, err
	}

	if int64(sizeBytes) != INT_SIZE_BYTES {
		panic("The raftlog may be corrupt, cannot proceed")
	}

	// Decode bytes as int, which is sizeof(LogEntry).
	size, err := decodeSizeBytes(b)
	if err != nil {
		return -1, err
	}

	return size, nil
}

func readLogEntry(f *os.File, size int) (*LogEntry, error) {
	b := make([]byte, size)
	leSize, err := f.Read(b)

	if err != nil {
		return nil, err
	}

	if leSize != size {
		panic("The raftlog may be corrupt, cannot proceed")
	}

	buff := bytes.NewBuffer(b)
	var entry LogEntry
	dataDecoder := gob.NewDecoder(buff)
	err = dataDecoder.Decode(&entry)

	if err != nil {
		return nil, err
	}

	return &entry, nil
}

////////////////////////////////////////////////////////////////////////////////
// Utilities for file I/O                                                     //
////////////////////////////////////////////////////////////////////////////////

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	} else if os.IsNotExist(err) {
		return false
	} else {
		panic(err)
	}
}

func getFileInfo(filename string) (int64, bool) {
	stat, err := os.Stat(filename)
	if err == nil {
		return stat.Size(), true
	} else if os.IsNotExist(err) {
		return 0, false
	} else {
		panic(err)
	}
}

func copyFile(srcFile string, dstFile string) error {
	src, err := os.Open(srcFile)
	if err != nil {
		return err
	}

	dst, err := os.Create(dstFile)
	if err != nil {
		return err
	}

	_, err = io.Copy(dst, src)
	if err != nil {
		return err
	}

	err = src.Close()
	if err != nil {
		return fmt.Errorf("Error closing src file: %v", err)
	}

	err = dst.Close()
	if err != nil {
		return fmt.Errorf("Error closing dst file: %v", err)
	}
	return nil
}

// RemoveLogs deletes the current node's logs from disk.
func (r *RaftNode) RemoveLogs() error {
	r.raftLogFd.fd.Close()
	r.raftLogFd.open = false
	err := os.Remove(r.raftLogFd.filename)
	if err != nil {
		r.Error("Unable to remove raftlog file")
		return err
	}

	r.raftMetaFd.fd.Close()
	r.raftMetaFd.open = false
	err = os.Remove(r.raftMetaFd.filename)
	if err != nil {
		r.Error("Unable to remove raftmeta file")
		return err
	}

	return nil
}
