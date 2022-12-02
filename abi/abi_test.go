package abi

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"testing"
)

func TestPad(t *testing.T) {
	r := []byte{0x20}
	rwant := make([]byte, 32)
	rwant[0] = 0x20
	if !bytes.Equal(rpad(r), rwant) {
		t.Errorf("want: %x got: %x", rwant, rpad(r))
	}

	b := make([]byte, 33)
	rand.Read(b[:])
	bwant := make([]byte, 64)
	copy(bwant, b)
	if !bytes.Equal(bwant, rpad(b)) {
		t.Errorf("want: %x got: %x", bwant, rpad(b))
	}
}

// Test vector taken from:
// https://docs.soliditylang.org/en/latest/abi-spec.html#examples
func TestEncode(t *testing.T) {
	want, _ := hex.DecodeString(`0000000000000000000000000000000000000000000000000000000000000060000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000000000000000000000000000000000000000000464617665000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000003`)
	got := Encode(String("dave"), Bool(true), List(Int(1), Int(2), Int(3)))
	if !bytes.Equal(want, got) {
		t.Errorf("want: %x got: %x", want, got)
	}
}

// Test vector taken from:
// https://docs.soliditylang.org/en/latest/abi-spec.html#use-of-dynamic-types
func TestEncode_Nested(t *testing.T) {
	want, _ := hex.DecodeString(`000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000030000000000000000000000000000000000000000000000000000000000000003000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000e000000000000000000000000000000000000000000000000000000000000000036f6e650000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000374776f000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000057468726565000000000000000000000000000000000000000000000000000000`)
	got := Encode(
		List(List(Int(1), Int(2)), List(Int(3))),
		List(String("one"), String("two"), String("three")),
	)
	if !bytes.Equal(want, got) {
		t.Errorf("want: %x got: %x", want, got)
	}
}

func TestDecode(t *testing.T) {
	a, b := [20]byte{}, [20]byte{}
	a[19] = 1
	b[19] = 2
	want := []Item{
		Address(a),
		Address(b),
		Int(1),
	}
	debug(t, Encode(want...))
	got := Decode(Encode(want...), AddressType, AddressType, IntType)
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want: %# v got: %# v", want, got)
	}
}

func TestDecode_Dynamic(t *testing.T) {
	want := List(String("hello"), String("world"))
	enc := Encode(want)
	got := Decode(enc, StringType, StringType)
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want: %# v got: %# v", want, got)
	}
}

func debug(t *testing.T, b []byte) []byte {
	t.Helper()
	out := fmt.Sprintf("len: %d\n", len(b))
	for i := 0; i < len(b); i += 32 {
		out += fmt.Sprintf("%x\n", b[i:i+32])
	}
	t.Logf("debug:\n%s\n", out)
	return b
}
