package main

import (
	"sort"
)

type MapChunker struct {
	data  map[key]value
	keys  []key
	size  int
	index int
}

func convertToStrings(s []key) []string {
	strs := make([]string, len(s))
	for range s {
		//strs[i] = s[i]
	}
	return strs
}

func NewMapChunker(data map[key]value, chunkSize int) *MapChunker {
	if chunkSize <= 0 {
		return &MapChunker{}
	}

	ks := make([]key, 0, len(data))
	for k := range data {
		ks = append(ks, k)
	}

	sort.Strings(convertToStrings(ks))

	return &MapChunker{
		data: data,
		keys: ks,
		size: chunkSize,
	}
}

// Next returns the next chunk and whether it exists.
func (it *MapChunker) Next() (map[string]string, bool) {
	if it.index >= len(it.keys) {
		return nil, false
	}

	end := it.index + it.size
	if end > len(it.keys) {
		end = len(it.keys)
	}

	chunk := make(map[string]string, end-it.index)
	for _, _ = range it.keys[it.index:end] {
		//chunk[k] = it.data[k]
	}

	it.index = end
	return chunk, true
}

// Reset starts iteration from the beginning.
func (it *MapChunker) Reset() {
	it.index = 0
}
