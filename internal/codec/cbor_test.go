package codec

import (
	"reflect"
	"testing"
)

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	type sample struct {
		ID    string
		Count int
		Tags  []string
	}
	want := sample{ID: "abc", Count: 42, Tags: []string{"x", "y"}}
	bytes, err := Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got sample
	if err := Unmarshal(bytes, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID != want.ID || got.Count != want.Count {
		t.Fatalf("roundtrip mismatch: %+v != %+v", got, want)
	}
	if len(got.Tags) != len(want.Tags) {
		t.Fatalf("roundtrip tags mismatch: %+v != %+v", got.Tags, want.Tags)
	}
	for i := range got.Tags {
		if got.Tags[i] != want.Tags[i] {
			t.Fatalf("roundtrip tag %d mismatch: %s != %s", i, got.Tags[i], want.Tags[i])
		}
	}
}

func TestUnmarshalIndefiniteLength(t *testing.T) {
	// Indefinite-length CBOR array [1,2]
	payload := []byte{0x9f, 0x01, 0x02, 0xff}
	var out []int
	if err := Unmarshal(payload, &out); err != nil {
		t.Fatalf("decode indefinite array: %v", err)
	}
	if len(out) != 2 || out[0] != 1 || out[1] != 2 {
		t.Fatalf("unexpected output: %#v", out)
	}
}

func TestUnmarshalInterfaceUsesStringMaps(t *testing.T) {
	bytes, err := Marshal(map[string]any{
		"result": map[string]any{
			"items": []any{map[string]any{"id": "item-1"}},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got any
	if err := Unmarshal(bytes, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := map[string]any{"result": map[string]any{"items": []any{map[string]any{"id": "item-1"}}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected output: %#v", got)
	}
}
