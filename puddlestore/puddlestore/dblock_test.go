package puddlestore

import (
	"bytes"
	"testing"
)

func perror(err error, t *testing.T) {
	if err != nil {
		t.Errorf("%v", err)
	}
}

func EncodeDecodeBlock(db *DBlock, t *testing.T) {
	bits, err := db.encode()
	perror(err, t)
	decoded, err := decodeDBlock(bits)
	perror(err, t)

	if decoded.BGUID != db.BGUID {
		t.Errorf("guids don't match")
	}

	if bytes.Compare(db.Data, decoded.Data) != 0 {
		t.Errorf("Data doesn't match")
	}
}

func TestEncoding(t *testing.T) {
	db := newDBlock()
	db.Data = []byte("random slice of bytes here")
	EncodeDecodeBlock(db, t)
}

func TestDBlockEncodingNoGuidNoData(t *testing.T) {
	db := newDBlock()
	db.Data = nil
	db.BGUID = ""
	EncodeDecodeBlock(db, t)
}

func TestDBlockEncodingNoGuid(t *testing.T) {
	db := newDBlock()
	db.Data = []byte("other random slice")
	db.BGUID = ""
	EncodeDecodeBlock(db, t)
}

func TestDBlockEncodingNoData(t *testing.T) {
	db := newDBlock()
	db.Data = nil
	EncodeDecodeBlock(db, t)
}
