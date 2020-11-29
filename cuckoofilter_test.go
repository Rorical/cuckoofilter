package cuckoo

import (
	"testing"
)

func TestInsertion(t *testing.T) {
	cf := NewFilter(8, "gogogo")
	t.Error(cf.Count())
	/*
		for i := 0; i < 10000; i++ {
			success := cf.Insert([]byte(fmt.Sprint(i)))
			if !success {
				t.Error(cf.count)
			}
		}
		defer cf.SaveFile()
	*/
}
