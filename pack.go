// SPDX-FileCopyrightText: Copyright 2025 Carabiner Systems, Inc
// SPDX-License-Identifier: Apache-2.0

package jsonl

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"sigs.k8s.io/release-utils/helpers"
)

// PackFilesToBundle creates the jsonl file at bundle and writes to it the
// flattened versions of the JSON files passed in the paths.
func PackFilesToBundle(bundle string, paths []string) error {
	f, err := os.Create(bundle)
	if err != nil {
		return fmt.Errorf("opening bundle file: %w", err)
	}
	defer f.Close() //nolint:errcheck

	return PackFilesToStream(f, paths)
}

// PackFilesToStream takes a writer and writes to it the flattened versions
// of the JSON files passed in the paths, one document per line. A directory
// contributes every .json file at its top level. A file holding more than one
// JSON document, such as a jsonl file, contributes one line per document.
func PackFilesToStream(w io.Writer, paths []string) error {
	for _, path := range paths {
		if helpers.IsDir(path) {
			if err := FlattenJSONDirectoryToWriter(w, path); err != nil {
				return err
			}
			continue
		}

		if err := packFile(w, path); err != nil {
			return err
		}
	}
	return nil
}

// PackDocumentsToStream reads JSON documents from r and writes each of them
// to w flattened into a single line. The reader may hold one document spanning
// several lines (a pretty-printed bundle), one document per line (a jsonl
// stream) or any other concatenation of JSON documents. Data that does not
// parse as JSON is an error.
func PackDocumentsToStream(w io.Writer, r io.Reader) error {
	dec := json.NewDecoder(r)
	for {
		var doc json.RawMessage
		if err := dec.Decode(&doc); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("parsing json document: %w", err)
		}

		line, err := FlattenJSON(doc)
		if err != nil {
			return fmt.Errorf("flattening json document: %w", err)
		}
		if _, err := w.Write(line); err != nil {
			return fmt.Errorf("writing document: %w", err)
		}
		if _, err := io.WriteString(w, "\n"); err != nil {
			return fmt.Errorf("writing document: %w", err)
		}
	}
}

// packFile writes the JSON documents found in the file at path to w, one per
// line.
func packFile(w io.Writer, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening %q: %w", path, err)
	}
	defer f.Close() //nolint:errcheck

	if err := PackDocumentsToStream(w, f); err != nil {
		return fmt.Errorf("packing %q: %w", path, err)
	}
	return nil
}
