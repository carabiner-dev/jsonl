// SPDX-FileCopyrightText: Copyright 2025 Carabiner Systems, Inc
// SPDX-License-Identifier: Apache-2.0

package jsonl

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIterartor(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		bundle    string
		numFiles  int
		numErrors int
	}{
		{"real-bundle", "testdata/attestations.jsonl", 7, 0},
		{"invalid-line", "testdata/onebad.jsonl", 7, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data, err := os.Open(tc.bundle)
			require.NoError(t, err)
			defer data.Close()
			var c, e int
			for i, r := range IterateBundle(data) {
				c++
				if r == nil {
					e++
					continue
				}
				d, err := io.ReadAll(r)
				require.NoError(t, err)
				fmt.Printf("\n##########################\nATT %d:\n%s\n", i, string(d))
			}
			require.Equal(t, tc.numFiles, c)
			require.Equal(t, tc.numErrors, e)
		})
	}
}
