// SPDX-FileCopyrightText: Copyright 2026 Carabiner Systems, Inc
// SPDX-License-Identifier: Apache-2.0

package jsonl

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testdataDir    = "testdata"
	testBundlePath = testdataDir + "/test.bundle"
	testJSONLPath  = testdataDir + "/attestations.jsonl"
	testOneBadPath = testdataDir + "/onebad.jsonl"
	notJSON        = "not json\n"
)

// copyFile copies src into dir under name.
func copyFile(t *testing.T, src, dir, name string) {
	t.Helper()
	data, err := os.ReadFile(src)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o600)) //nolint:gosec // test helper, dir is a t.TempDir()
}

// bundleDir builds a directory holding two bundles plus entries that packing
// a directory must skip: a jsonl file, a non-JSON file and a subdirectory.
func bundleDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	copyFile(t, testBundlePath, dir, "a.json")
	copyFile(t, testBundlePath, dir, "b.json")
	copyFile(t, testJSONLPath, dir, "ignored.jsonl")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte(notJSON), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o750))
	copyFile(t, testBundlePath, filepath.Join(dir, "sub"), "nested.json")
	return dir
}

// requireJSONLines checks that data holds exactly numLines newline-terminated
// lines, each of them a single JSON document, and returns the parsed documents.
func requireJSONLines(t *testing.T, data string, numLines int) []any {
	t.Helper()
	require.True(t, strings.HasSuffix(data, "\n"), "output must end with a newline")
	lines := strings.Split(strings.TrimSuffix(data, "\n"), "\n")
	require.Len(t, lines, numLines)
	docs := make([]any, 0, len(lines))
	for i, line := range lines {
		require.NotEmpty(t, strings.TrimSpace(line), "line %d is blank", i)
		dec := json.NewDecoder(strings.NewReader(line))
		var doc any
		require.NoError(t, dec.Decode(&doc), "line %d is not JSON", i)
		require.False(t, dec.More(), "line %d holds more than one document", i)
		docs = append(docs, doc)
	}
	return docs
}

func TestPackDocumentsToStream(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		input    func(t *testing.T) []byte
		numLines int
		mustErr  bool
	}{
		{"pretty-printed", func(t *testing.T) []byte {
			t.Helper()
			data, err := os.ReadFile(testBundlePath)
			require.NoError(t, err)
			return data
		}, 1, false},
		{"jsonl", func(t *testing.T) []byte {
			t.Helper()
			data, err := os.ReadFile(testJSONLPath)
			require.NoError(t, err)
			return data
		}, 7, false},
		{"concatenated", func(t *testing.T) []byte {
			t.Helper()
			return []byte("{\"a\": 1}\n{\n  \"b\": 2\n} {\"c\": 3}")
		}, 3, false},
		{"empty", func(t *testing.T) []byte { t.Helper(); return []byte("\n\n") }, 0, false},
		{"invalid", func(t *testing.T) []byte { t.Helper(); return []byte("{\"a\": 1}\nnot json\n") }, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := tc.input(t)
			var out bytes.Buffer
			err := PackDocumentsToStream(&out, bytes.NewReader(input))
			if tc.mustErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.numLines == 0 {
				require.Empty(t, out.String())
				return
			}
			got := requireJSONLines(t, out.String(), tc.numLines)

			// Every document must survive the round trip unchanged.
			dec := json.NewDecoder(bytes.NewReader(input))
			for i, doc := range got {
				var want any
				require.NoError(t, dec.Decode(&want))
				if !reflect.DeepEqual(doc, want) {
					t.Errorf("document %d differs from its source", i)
				}
			}
		})
	}
}

func TestPackFilesToStream(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		paths    func(t *testing.T) []string
		numLines int
		mustErr  bool
	}{
		{"pretty-printed-bundle", func(t *testing.T) []string { t.Helper(); return []string{testBundlePath} }, 1, false},
		{"jsonl", func(t *testing.T) []string { t.Helper(); return []string{testJSONLPath} }, 7, false},
		{"directory", func(t *testing.T) []string { t.Helper(); return []string{bundleDir(t)} }, 2, false},
		{"mixed", func(t *testing.T) []string {
			t.Helper()
			return []string{bundleDir(t), testJSONLPath, testBundlePath}
		}, 10, false},
		{"bad-line-in-jsonl", func(t *testing.T) []string { t.Helper(); return []string{testOneBadPath} }, 0, true},
		{"invalid-file-in-directory", func(t *testing.T) []string {
			t.Helper()
			dir := t.TempDir()
			copyFile(t, testBundlePath, dir, "a.json")
			require.NoError(t, os.WriteFile(filepath.Join(dir, "report.json"), []byte(notJSON), 0o600))
			return []string{dir}
		}, 0, true},
		{"missing-file", func(t *testing.T) []string { t.Helper(); return []string{"testdata/non-existent.json"} }, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out bytes.Buffer
			err := PackFilesToStream(&out, tc.paths(t))
			if tc.mustErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			requireJSONLines(t, out.String(), tc.numLines)
		})
	}
}

func TestPackFilesToBundle(t *testing.T) {
	t.Parallel()
	bundle := filepath.Join(t.TempDir(), "attestations.jsonl")
	require.NoError(t, PackFilesToBundle(bundle, []string{testBundlePath, testJSONLPath}))

	data, err := os.ReadFile(bundle)
	require.NoError(t, err)
	requireJSONLines(t, string(data), 8)
}
