// SPDX-FileCopyrightText: Copyright 2025 Carabiner Systems, Inc
// SPDX-License-Identifier: Apache-2.0

package jsonl

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const defaultFilePrefix = "jsondata-"

// UnpackBundleFile reads a jsonl file and extracts attestations bundled in it.
func UnpackBundleFile(path string, fnOpts ...unpackoptFunc) error {
	opts := unpackOptions{}
	for _, fn := range fnOpts {
		if err := fn(&opts); err != nil {
			return err
		}
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening jsonl bundle: %w", err)
	}
	defer f.Close() //nolint:errcheck

	// Compute the prefix from the file
	if opts.filePrefix == "" {
		fileNameBase := filepath.Base(path)
		if strings.HasSuffix(fileNameBase, "jsonl") {
			fileNameBase = strings.TrimSuffix(fileNameBase, "l")
		}
		fileNameBase = strings.TrimSuffix(fileNameBase, ".json")
		fileNameBase = strings.TrimSuffix(fileNameBase, ".bundle")
		fnOpts = append(fnOpts, WithFilePrefix(fileNameBase))
	}

	return UnpackBundle(f, fnOpts...)
}

// UnpackBundle reads data from the r io.Reader and writes each json document
// to a separate file.
func UnpackBundle(r io.Reader, fnOpts ...unpackoptFunc) error {
	opts := unpackOptions{}
	for _, fn := range fnOpts {
		if err := fn(&opts); err != nil {
			return err
		}
	}

	prefix := opts.filePrefix
	if prefix == "" || prefix == "." {
		prefix = defaultFilePrefix
	}

	outDir := opts.outDirectory
	if outDir == "" {
		outDir = "."
	}

	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024)

	// Increase the buf max value to 10 mb just in case
	// we encounter huge lines
	scanner.Buffer(buf, 1024*1024*10)

	i := -1
	for scanner.Scan() {
		i++
		data := scanner.Bytes()
		var parsed any
		if err := json.Unmarshal(data, &parsed); err != nil {
			if opts.failOnInvalid {
				return fmt.Errorf("invalid json document on line %d", i)
			}
			// If the setting is not on, just ignore the line
			continue
		}
		if err := os.WriteFile(
			filepath.Join(outDir, fmt.Sprintf("%s%02d.json", prefix, i)),
			data, os.FileMode(0o644),
		); err != nil {
			return fmt.Errorf("writing document #%d: %w", i, err)
		}
	}
	return nil
}
