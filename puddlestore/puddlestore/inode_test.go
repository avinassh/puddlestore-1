package puddlestore

import "testing"

func TestEncodeDecodeINode(t *testing.T) {
	in := &INode{nil, "cats", "are", "cool", FILE, 0, []string{"don't", "you", "think"}}
	bits, err := in.encode()
	perror(err, t)
	decoded, err := decodeInode(bits, in.conn)
	perror(err, t)

	if decoded.AGUID != in.AGUID {
		t.Errorf("guids don't match")
	}

	if decoded.VGUID != in.VGUID {
		t.Errorf("guids don't match")
	}

	if decoded.GetName() != in.GetName() {
		t.Errorf("names don't match")
	}

	if decoded.Class != in.Class {
		t.Errorf("names don't match")
	}

	for i, id := range in.Blocks {
		if id != decoded.Blocks[i] {
			t.Errorf("aguid lists differ")
		}
	}
}
