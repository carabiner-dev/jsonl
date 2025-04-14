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

// FlattenJSONDirectoryToWriter flattens all JSON files in a directory
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

		f, err := os.Open(filepath.Join(path, entry.Name()))
		if err != nil {
			return fmt.Errorf("opening file: %w", err)
		}
		defer f.Close() //nolint:errcheck

		if _, err := io.Copy(w, FlattenJSONStream(f)); err != nil {
			return fmt.Errorf("writing stream")
		}
		if _, err := w.Write([]byte("\n")); err != nil {
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
