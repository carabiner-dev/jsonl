// SPDX-FileCopyrightText: Copyright 2025 Carabiner Systems, Inc
// SPDX-License-Identifier: Apache-2.0

package jsonl

// unpackOptions capture the switches for extracting data from jsonl bundles
type unpackOptions struct {
	failOnInvalid bool
	filePrefix    string
	outDirectory  string
}

type unpackoptFunc func(*unpackOptions) error

// WithFailOnInvalid makes the extractor fail if there is an unparseable line
func WithFailOnInvalid(yesno bool) unpackoptFunc {
	return func(uo *unpackOptions) error {
		uo.failOnInvalid = yesno
		return nil
	}
}

// WithFilePrefix specifies a prefix to use in the filenames
func WithOutputDirectory(path string) unpackoptFunc {
	return func(uo *unpackOptions) error {
		uo.outDirectory = path
		return nil
	}
}

// WithFilePrefix specifies a prefix to use in the filenames
func WithFilePrefix(prefix string) unpackoptFunc {
	return func(uo *unpackOptions) error {
		uo.filePrefix = prefix
		return nil
	}
}
