// This file is adapted from the Go standard library's encoding/csv package
// (https://pkg.go.dev/encoding/csv), which is distributed under the
// following license:
//
// Copyright 2009 The Go Authors.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//   - Redistributions of source code must retain the above copyright
//     notice, this list of conditions and the following disclaimer.
//   - Redistributions in binary form must reproduce the above
//     copyright notice, this list of conditions and the following disclaimer
//     in the documentation and/or other materials provided with the
//     distribution.
//   - Neither the name of Google LLC nor the names of its
//     contributors may be used to endorse or promote products derived from
//     this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// The only behavioral differences from the standard library's csv.Writer
// are two deliberately opt-in extensions: EscapeCharacter (the sequence
// written before an embedded quote inside a quoted field — RFC 4180 always
// doubles the quote, and that remains the default here) and
// AlwaysEncapsulate (quote every field unconditionally, instead of only
// when a field actually requires it). Everything else — quoting rules,
// delimiter handling, line endings, including the standard library's own
// dropped-bare-\r-in-CRLF-mode behavior — is unchanged from upstream.

package filesink

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

var errInvalidCSVDelim = errors.New("filesink: invalid csv field delimiter")

func validCSVDelim(r rune) bool {
	return r != 0 && r != '"' && r != '\r' && r != '\n' && utf8.ValidRune(r) && r != utf8.RuneError
}

// csvWriter is a fork of encoding/csv.Writer; see the file-level comment
// for what's changed and what's deliberately identical to upstream.
type csvWriter struct {
	Comma   rune // Field delimiter (set to ',' by newCSVWriter)
	UseCRLF bool // True to use \r\n as the line terminator

	// EscapeCharacter is written before an embedded quote character inside
	// a quoted field. RFC 4180 specifies doubling the quote character,
	// which is what an empty EscapeCharacter (the default) does. Set this
	// only to interoperate with a consumer that expects a different
	// escaping convention (e.g. a backslash) — output written with a
	// non-default EscapeCharacter is not RFC 4180-compliant and will not
	// round-trip through a standard CSV reader.
	EscapeCharacter string

	// AlwaysEncapsulate quotes every field unconditionally, instead of
	// only fields that actually require it (those containing the
	// delimiter, a quote character, \r, \n, or leading whitespace).
	AlwaysEncapsulate bool

	w *bufio.Writer
}

func newCSVWriter(w io.Writer) *csvWriter {
	return &csvWriter{
		Comma: ',',
		w:     bufio.NewWriter(w),
	}
}

func (w *csvWriter) escapeSequence() string {
	if w.EscapeCharacter == "" {
		return `"`
	}
	return w.EscapeCharacter
}

// Write writes a single CSV record with quoting and escaping applied as
// configured.
func (w *csvWriter) Write(record []string) error {
	if !validCSVDelim(w.Comma) {
		return errInvalidCSVDelim
	}

	for n, field := range record {
		if n > 0 {
			if _, err := w.w.WriteRune(w.Comma); err != nil {
				return err
			}
		}

		if !w.fieldNeedsQuotes(field) {
			if _, err := w.w.WriteString(field); err != nil {
				return err
			}
			continue
		}

		if err := w.w.WriteByte('"'); err != nil {
			return err
		}
		esc := w.EscapeCharacter
		for len(field) > 0 {
			i := strings.IndexAny(field, "\"\r\n")
			// When a non-default EscapeCharacter is set, a literal
			// occurrence of it inside the field is just as ambiguous to an
			// escape-based reader as an unescaped quote: it must be escaped
			// too, or the reader can't tell it apart from an
			// escape-introducer byte. Treat the earliest occurrence of
			// either as the next thing to handle.
			escAt := -1
			if esc != "" {
				escAt = strings.Index(field, esc)
				if escAt >= 0 && (i < 0 || escAt < i) {
					i = escAt
				}
			}
			if i < 0 {
				i = len(field)
			}

			if _, err := w.w.WriteString(field[:i]); err != nil {
				return err
			}
			field = field[i:]

			if len(field) > 0 {
				if esc != "" && escAt == i && strings.HasPrefix(field, esc) {
					if _, err := w.w.WriteString(esc + esc); err != nil {
						return err
					}
					field = field[len(esc):]
					continue
				}

				var err error
				switch field[0] {
				case '"':
					_, err = w.w.WriteString(w.escapeSequence() + `"`)
				case '\r':
					if !w.UseCRLF {
						err = w.w.WriteByte('\r')
					}
				case '\n':
					if w.UseCRLF {
						_, err = w.w.WriteString("\r\n")
					} else {
						err = w.w.WriteByte('\n')
					}
				}
				field = field[1:]
				if err != nil {
					return err
				}
			}
		}
		if err := w.w.WriteByte('"'); err != nil {
			return err
		}
	}

	var err error
	if w.UseCRLF {
		_, err = w.w.WriteString("\r\n")
	} else {
		err = w.w.WriteByte('\n')
	}
	return err
}

func (w *csvWriter) Flush() {
	w.w.Flush()
}

func (w *csvWriter) Error() error {
	_, err := w.w.Write(nil)
	return err
}

// fieldNeedsQuotes reports whether field must be enclosed in quotes.
func (w *csvWriter) fieldNeedsQuotes(field string) bool {
	if w.AlwaysEncapsulate {
		return true
	}
	if field == "" {
		return false
	}

	if field == `\.` {
		return true
	}

	if w.Comma < utf8.RuneSelf {
		for i := 0; i < len(field); i++ {
			c := field[i]
			if c == '\n' || c == '\r' || c == '"' || c == byte(w.Comma) {
				return true
			}
		}
	} else {
		if strings.ContainsRune(field, w.Comma) || strings.ContainsAny(field, "\"\r\n") {
			return true
		}
	}

	r1, _ := utf8.DecodeRuneInString(field)
	return unicode.IsSpace(r1)
}
