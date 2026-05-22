package sqlite

import "strings"

// StripGoose returns only the SQL between "-- +goose Up" and "-- +goose Down"
// markers in a goose migration file. Used by migration and test helpers.
func StripGoose(s string) string {
	var b strings.Builder
	in := false
	for line := range strings.SplitSeq(s, "\n") {
		switch {
		case strings.TrimSpace(line) == "-- +goose Up":
			in = true
		case strings.TrimSpace(line) == "-- +goose Down":
			in = false
		case in:
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}
	return b.String()
}
