package keyvaluestore

import (
	"errors"
	"fmt"
	"strings"
)

const (
	SET    = 0 // SET "KEY:VALUE"
	DELETE = 1 // DELETE "KEY"
	GET    = 2 // GET "KEY"
)

/*
OPERATIONS

SET:
input: byte array describing string "key:value"
output: success message

DELETE
input: []byte describing string "key"
output: success message

GET
input: []byte describing string "key"
output: string representing the value in the KVstore
NOTE: returns "" but NO ERROR if key not found!
*/

// KeyValueStore implements the raft.StateMachine interface, and represents a
// key value store from strings to strings (AGUID --> VGUID)
type KeyValueStore struct {
	store map[string]string
}

/*
	Creates the internal state of the KeyValueStore if not already created
*/
func (kv *KeyValueStore) initState() {
	if kv.store == nil {
		kv.store = make(map[string]string)
	}
}

// GetState returns the state of the state machine as an interface{}, which can
// be converted to the expected type using type assertions.
func (kv *KeyValueStore) GetState() (state interface{}) {
	kv.initState()
	return kv.store
}

// ApplyCommand applies the given state machine command to the KeyValueStore, and
// returns a message and an error if there is one.
func (kv *KeyValueStore) ApplyCommand(command uint64, data []byte) (message string, err error) {
	kv.initState()

	switch command {
	case SET:
		args := strings.Split(string(data), ":")
		if len(args) < 2 {
			return "Not enough args provided", errors.New("2 args required for setting")
		}
		key := args[0]
		val := args[1]
		kv.store[key] = val
		return fmt.Sprintf("Successfully set %v:%v", key, val), nil
	case DELETE:
		key := string(data)
		delete(kv.store, key)
		return fmt.Sprintf("Successfully deleted %v", key), nil
	case GET:
		key := string(data)
		if val, ok := kv.store[key]; ok {
			return val, nil
		} else {
			return "", nil
		}
	default:
		return "", errors.New("unknown command type")
	}
}

// FormatCommand returns the string representation of a KeyValueStore state machine command.
func (kv *KeyValueStore) FormatCommand(command uint64) (commandString string) {
	kv.initState()

	switch command {
	case SET:
		return "SET"
	case DELETE:
		return "DELETE"
	case GET:
		return "GET"
	default:
		return "UNKNOWN_COMMAND"
	}
}

func (kv KeyValueStore) String() string {
	kv.initState()
	return fmt.Sprintf("KeyValueStore{%v}", kv.store)
}
