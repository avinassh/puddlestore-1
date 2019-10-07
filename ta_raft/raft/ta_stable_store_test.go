package raft

import (
	"fmt"
	"os"
	"os/user"
	"runtime"
	"testing"
)

func TestStableStore(t *testing.T) {
	// Use a path in /tmp so we use local disk and not NFS
	logPath := "raftlogs"
	if runtime.GOOS != "windows" {
		curUser, _ := user.Current()
		logPath = "/tmp/" + curUser.Username + "_raftlogs"
	}

	t.Run("error", func(t *testing.T) {
		testError(t, logPath)
	})

	t.Run("backup", func(t *testing.T) {
		testBackup(t, logPath)
	})

	t.Run("basic", func(t *testing.T) {
		testBasic(t, logPath)
	})

	t.Run("truncate", func(t *testing.T) {
		testTruncate(t, logPath)
	})
}

func testError(t *testing.T, logPath string) {
	raftLogFD := FileData{filename: logPath + "/err_test_raftlog.dat"}
	err := CreateRaftLog(&raftLogFD)
	if err != nil {
		t.Errorf("error when creating raftlog %v", err)
	}

	err = raftLogFD.fd.Close()
	if err != nil {
		t.Errorf("error when closing raftlog %v", err)
	}

	err = raftLogFD.fd.Close()
	if err == nil {
		t.Errorf("we should get an error when closing raftlog twice! %v", err)
	}

	err = os.Remove(raftLogFD.filename)
	if err != nil {
		t.Errorf("Unable to remove temp testing file")
	}
}

func testBackup(t *testing.T, logPath string) {
	raftMetaFD := FileData{filename: logPath + "/basic_test_raftmeta.dat"}
	err := CreateStableState(&raftMetaFD)
	if err != nil {
		t.Errorf("error when creating metalog %v", err)
	}

	ss := StableState{}
	ss.CurrentTerm = uint64(2)
	ss.VotedFor = "123"

	err = WriteStableState(&raftMetaFD, ss)
	if err != nil {
		t.Errorf("error when writing out metalog %v", err)
	}

	ss.CurrentTerm = uint64(3)
	ss.VotedFor = "246"

	err = WriteStableState(&raftMetaFD, ss)
	if err != nil {
		t.Errorf("error when writing out 2nd metalog %v", err)
	}

	err = raftMetaFD.fd.Close()
	if err != nil {
		t.Errorf("Unable to close metalog: %v", err)
	}

	ss3, err := ReadStableState(&raftMetaFD)
	if err != nil {
		t.Errorf("Unable to read stable state file %v", err)
	}

	if ss3.CurrentTerm != uint64(3) || ss3.VotedFor != "246" {
		t.Errorf("term should equal 3 (was %v), votedfor should be 246 (was %v)", ss3.CurrentTerm, ss3.VotedFor)
	}

	err = os.Remove(raftMetaFD.filename)
	if err != nil {
		t.Errorf("Unable to remove temp testing file: %v", err)
	}
}

func testBasic(t *testing.T, logPath string) {
	raftLogFD := FileData{filename: logPath + "/basic_test_raftlog.dat"}
	err := CreateRaftLog(&raftLogFD)
	if err != nil {
		t.Errorf("error when creating raftlog %v", err)
	}

	entry := LogEntry{
		Index:  0,
		TermId: 0,
		Type:   CommandType_INIT,
		Data:   []byte{0},
	}

	// post: indices of entries: 0,1,2,3
	for i := 0; i < 4; i++ {
		entry.Index = uint64(i)
		err = AppendLogEntry(&raftLogFD, &entry)
		if err != nil {
			t.Errorf("error when appending entry %v", err)
		}
	}

	err = raftLogFD.fd.Close()
	if err != nil {
		t.Errorf("Error when closing file: %v", err)
	}

	correctIdx := []uint64{0, 1, 2, 3}
	entries, err := ReadRaftLog(&raftLogFD)
	if err != nil {
		t.Error(err.Error())
	}

	if len(correctIdx) != len(entries) {
		t.Errorf("Length of log entries is %v, but should be %v", len(entries), len(correctIdx))
	}
	for i, entry := range entries {
		if correctIdx[i] != entry.Index {
			t.Errorf("Entry index is %v, but should be %v", entry.Index, correctIdx[i])
		}
	}

	err = os.Remove(raftLogFD.filename)
	if err != nil {
		t.Errorf("Unable to remove temp testing file")
	}
}

func testTruncate(t *testing.T, logPath string) {
	raftLogFD := FileData{filename: logPath + "/truncate_test_raftlog.dat"}
	err := CreateRaftLog(&raftLogFD)
	if err != nil {
		t.Errorf("error when creating raftlog %v", err)
	}

	entry := LogEntry{
		Index:  0,
		TermId: 0,
		Type:   CommandType_INIT,
		Data:   []byte{0},
	}

	// post: indices of entries: 0,1,2,3
	for i := 0; i < 4; i++ {
		entry.Index = uint64(i)
		err = AppendLogEntry(&raftLogFD, &entry)
		if err != nil {
			t.Errorf("error when appending entry %v", err)
		}
	}

	// post: indices of entries: 0,1
	var truncIdx uint64 = 2
	newTruncSize := raftLogFD.idxMap[truncIdx]
	err = TruncateLog(&raftLogFD, truncIdx)
	if err != nil {
		t.Errorf("error when truncating raft log %v", err)
	}

	stat, err := os.Stat(raftLogFD.filename)
	if err != nil {
		t.Errorf("error when stat-ing raft log %v", err)
	}

	size := stat.Size()
	if size != newTruncSize {
		t.Errorf("expected new trunc size to be %v but was %v", newTruncSize, size)
	}
	fmt.Printf("Truncating raftlog to %v\n", size)

	// post: indices of entries: 0,1,0,1,2,3
	for i := 0; i < 4; i++ {
		entry.Index = uint64(i)
		err = AppendLogEntry(&raftLogFD, &entry)
		if err != nil {
			t.Errorf("error when appending entry %v", err)
		}
	}

	err = raftLogFD.fd.Close()
	if err != nil {
		t.Errorf("Error when closing file: %v", err)
	}

	correctIdx := []uint64{0, 1, 0, 1, 2, 3}
	entries, err := ReadRaftLog(&raftLogFD)
	if err != nil {
		t.Error(err.Error())
	}

	if len(correctIdx) != len(entries) {
		t.Errorf("Length of log entries is %v, but should be %v", len(entries), len(correctIdx))
	}
	for i, entry := range entries {
		if correctIdx[i] != entry.Index {
			t.Errorf("Entry index is %v, but should be %v", entry.Index, correctIdx[i])
		}
	}

	err = os.Remove(raftLogFD.filename)
	if err != nil {
		t.Errorf("Unable to remove temp testing file")
	}
}
