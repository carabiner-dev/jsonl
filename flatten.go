// SPDX-FileCopyrightText: Copyright 2025 Carabiner Systems, Inc
// SPDX-License-Identifier: Apache-2.0

package jsonl

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// FlattenJSONDirectoryToWriter flattens all the .json files at the top level
// of a directory into w, one document per line. Subdirectories and files with
// other extensions (jsonl files included) are ignored.
func FlattenJSONDirectoryToWriter(w io.Writer, path string) error {
	dirContents, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("opening dir: %w", err)
	}

	for _, entry := range dirContents {
		if entry.IsDir() {
			continue
		}

		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		if err := packFile(w, filepath.Join(path, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

// FlattenJSON flattens a JSON document into a single line, suitable to add to
// a jsonl file.
func FlattenJSONStream(r io.Reader) io.Reader {
	newInput := ""
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024)
	// Increase the buf max value to 10 mb just in case
	// we encounter huge lines
	scanner.Buffer(buf, 1024*1024*10)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		newInput += line + " "
	}
	return strings.NewReader(newInput)
}

func FlattenJSON(data []byte) ([]byte, error) {
	r := FlattenJSONStream(bytes.NewReader(data))
	return io.ReadAll(r)
}
