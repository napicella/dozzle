package docker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/amir20/dozzle/internal/container"
	"github.com/rs/zerolog/log"
)

// journalEntry is the subset of fields we care about from journalctl --output json.
type journalEntry struct {
	// MESSAGE may be a string or, for binary payloads, a JSON array of byte values.
	Message           json.RawMessage `json:"MESSAGE"`
	RealtimeTimestamp string          `json:"__REALTIME_TIMESTAMP"`
}

// JournalLogReader implements container.LogReader for journalctl --output json output.
// Each line from journalctl is a self-contained JSON object. The reader converts it
// into the "<RFC3339Nano> <message>\n" format that EventGenerator.createEvent() expects.
type JournalLogReader struct {
	scanner *bufio.Scanner
}

// NewJournalLogReader creates a JournalLogReader that reads from r.
func NewJournalLogReader(r io.Reader) *JournalLogReader {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 64*1024), 1024*1024) // allow up to 1 MiB per line
	return &JournalLogReader{scanner: s}
}

// Read returns one log line at a time formatted as "<RFC3339Nano> <message>\n".
// It satisfies the container.LogReader interface.
func (j *JournalLogReader) Read() (string, container.StdType, error) {
	for {
		if !j.scanner.Scan() {
			if err := j.scanner.Err(); err != nil {
				return "", 0, err
			}
			return "", 0, io.EOF
		}

		line := j.scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry journalEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			// Not valid JSON (e.g. a journalctl status line) – skip it.
			log.Info().Msg("Message invalid json")
			continue
		}

		msg, err := extractMessage(entry.Message)
		if err != nil || msg == "" {
			continue
		}

		ts := parseTimestamp(entry.RealtimeTimestamp)

		// Format as the timestamp + space + message, matching what Docker's log stream
		// produces and what EventGenerator.createEvent() expects.
		formatted := fmt.Sprintf("%s %s\n", ts.Format(time.RFC3339Nano), msg)
		return formatted, container.STDOUT, nil
	}
}

// extractMessage handles MESSAGE being either a plain JSON string or a JSON array
// of byte values (journald's encoding for binary messages).
func extractMessage(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}

	// Try string first (most common case).
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, nil
	}

	// Try array of numbers (binary message encoded as []uint8).
	var bytes []uint8
	if err := json.Unmarshal(raw, &bytes); err == nil {
		return string(bytes), nil
	}

	return "", fmt.Errorf("unrecognised MESSAGE format")
}

// parseTimestamp converts a __REALTIME_TIMESTAMP string (microseconds since Unix epoch)
// to a time.Time. Falls back to time.Now() if parsing fails.
func parseTimestamp(raw string) time.Time {
	if raw == "" {
		return time.Now()
	}
	us, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Now()
	}
	return time.Unix(us/1_000_000, (us%1_000_000)*1000).UTC()
}
