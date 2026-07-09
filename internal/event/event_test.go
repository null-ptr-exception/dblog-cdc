package event

import "testing"

func TestEncodePK_NullByteCollision(t *testing.T) {
	// A single PK value containing \x00 collides with a compound PK
	// because EncodePK uses \x00 as the separator.
	single := EncodePK([]string{"a\x00b"})
	compound := EncodePK([]string{"a", "b"})

	if single == compound {
		t.Errorf("EncodePK collision: single PK %q == compound PK %q — different rows would map to same key",
			[]string{"a\x00b"}, []string{"a", "b"})
	}
}

func TestEncodePK_EmptySlice(t *testing.T) {
	result := EncodePK([]string{})
	if result != "" {
		t.Errorf("EncodePK([]) should be empty string, got %q", result)
	}
}

func TestEncodePK_SingleElement(t *testing.T) {
	result := EncodePK([]string{"42"})
	if result != "42" {
		t.Errorf("EncodePK([42]) should be '42', got %q", result)
	}
}

func TestEncodePK_EmptyStringElement(t *testing.T) {
	// An empty-string PK should not collide with other patterns
	a := EncodePK([]string{"", "b"})
	b := EncodePK([]string{"b"})
	if a == b {
		t.Errorf("EncodePK(['','b']) == EncodePK(['b']) — empty string PK causes collision")
	}
}
