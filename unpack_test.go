// SPDX-FileCopyrightText: Copyright 2025 Carabiner Systems, Inc
// SPDX-License-Identifier: Apache-2.0

package jsonl

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnpackBundle(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		bundle   string
		numFiles int
		optFuncs func() []unpackoptFunc
		mustErr  bool
	}{
		{"real-bundle", "testdata/attestations.jsonl", 7, nil, false},
		{"invalid-file", "testdata/non-existent.jsonl", 0, nil, true},
		{"invalid-line", "testdata/onebad.jsonl", 0, func() []unpackoptFunc { return []unpackoptFunc{WithFailOnInvalid(true)} }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			fns := []unpackoptFunc{}
			if tc.optFuncs != nil {
				fns = tc.optFuncs()
			}
			fns = append(fns, WithOutputDirectory(dir), WithFilePrefix(defaultFilePrefix))

			err := UnpackBundleFile(tc.bundle, fns...)
			if tc.mustErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			for i := range tc.numFiles {
				require.FileExists(t, filepath.Join(dir, fmt.Sprintf("%s%02d.json", defaultFilePrefix, i)))
			}
		})
	}
}
