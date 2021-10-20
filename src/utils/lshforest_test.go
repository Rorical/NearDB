package utils

import "testing"

func TestSliceDelete(t *testing.T) {
	t.Logf("%v", DeleteSlice([]string{"a", "b", "c", "d", "e"}, "c"))
}