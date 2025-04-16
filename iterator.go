// SPDX-FileCopyrightText: Copyright 2025 Carabiner Systems, Inc
// SPDX-License-Identifier: Apache-2.0

package jsonl

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"iter"
)

// IterateBundle implements an iterator that returns an io.Reader for
// each json file contained in a bundle read from reader r. The iteraror
// will loop exactly once for each line in the file.
//
// If a line contains data which cannot be parsed as json, the iterator will
// still loop but instead of an io.Reader the value will be nil.
func IterateBundle(r io.Reader) iter.Seq2[int, io.Reader] {
	return func(yield func(int, io.Reader) bool) {
		scanner := bufio.NewScanner(r)
		buf := make([]byte, 0, 64*1024)

		scanner.Buffer(buf, 1024*1024*10)

		i := -1
		for scanner.Scan() {
			i++
			data := scanner.Bytes()
			var parsed any
			if err := json.Unmarshal(data, &parsed); err != nil {
				if !yield(i, nil) {
					return
				}
				continue
			}
			if !yield(i, bytes.NewReader(data)) {
				return
			}
		}
	}
}
