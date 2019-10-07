package hashmachine

import (
	"crypto/md5"
	"errors"
	"fmt"
)

const (
	HASH_CHAIN_INIT uint64 = iota
	HASH_CHAIN_ADD
)

// HashMachine implements the raft.StateMachine interface, and represents a
// finite state machine storing a hash value. It stores a single hash value,
// which is updated by the successive application of the MD5 hash function.
type HashMachine struct {
	hash []byte
}

func (h *HashMachine) init(data []byte) (hash string, err error) {
	if len(h.hash) != 0 {
		return "", errors.New("the hash chain should only be initialized once")
	}

	h.hash = data

	return fmt.Sprintf("%v", h.hash), nil
}

func (h *HashMachine) add() (hash string, err error) {
	if len(h.hash) == 0 {
		return "", errors.New("the hash chain hasn't been initialized yet")
	}

	sum := md5.Sum(h.hash)
	fmt.Printf("Hash is changing from %v to %v\n", h.hash, sum)
	h.hash = sum[:]

	return fmt.Sprintf("%v", h.hash), nil
}

// GetState returns the state of the state machine as an interface{}, which can
// be converted to the expected type using type assertions.
func (h *HashMachine) GetState() (state interface{}) {
	return h.hash
}

// ApplyCommand applies the given state machine command to the HashMachine, and
// returns a message and an error if there is one.
func (h *HashMachine) ApplyCommand(command uint64, data []byte) (message string, err error) {
	switch command {
	case HASH_CHAIN_INIT:
		return h.init(data)
	case HASH_CHAIN_ADD:
		return h.add()
	default:
		return "", errors.New("unknown command type")
	}
}

// FormatCommand returns the string representation of a HashMachine state machine command.
func (h *HashMachine) FormatCommand(command uint64) (commandString string) {
	switch command {
	case HASH_CHAIN_INIT:
		return "HASH_CHAIN_INIT"
	case HASH_CHAIN_ADD:
		return "HASH_CHAIN_ADD"
	default:
		return "UNKNOWN_COMMAND"
	}
}

func (h HashMachine) String() string {
	return fmt.Sprintf("HashMachine{%v}", h.hash)
}
