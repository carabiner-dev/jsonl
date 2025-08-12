// SPDX-FileCopyrightText: Copyright 2025 Carabiner Systems, Inc
// SPDX-License-Identifier: Apache-2.0

package jsonl

import (
	"fmt"
	"io"
	"os"

	"sigs.k8s.io/release-utils/helpers"
)

// PackFilesToStream takes a writer and writes to it the flattened versions
// of the JSON files passed in the paths
func PackFilesToBundle(bundle string, paths []string) error {
	f, err := os.Create(bundle)
	if err != nil {
		return fmt.Errorf("opening bundle file: %w", err)
	}
	defer f.Close() //nolint:errcheck

	return PackFilesToStream(f, paths)
}

// PackFilesToStream takes a writer and writes to it the flattened versions
// of the JSON files passed in the paths
func PackFilesToStream(w io.Writer, paths []string) error {
	for _, path := range paths {
		if helpers.IsDir(path) {
			if err := FlattenJSONDirectoryToWriter(w, path); err != nil {
				return err
			}
		}
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("opening %q: %w", path, err)
		}
		if _, err := io.Copy(w, FlattenJSONStream(f)); err != nil {
			return fmt.Errorf("copying data to file: %w", err)
		}
		if _, err := io.WriteString(w, "\n"); err != nil {
			return err
		}
	}
	return nil
}
