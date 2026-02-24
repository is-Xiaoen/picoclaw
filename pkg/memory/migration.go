package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/providers"
)

// jsonSession mirrors the JSON structure used by pkg/session for deserialization.
type jsonSession struct {
	Key      string              `json:"key"`
	Messages []providers.Message `json:"messages"`
	Summary  string              `json:"summary,omitempty"`
	Created  time.Time           `json:"created"`
	Updated  time.Time           `json:"updated"`
}

// MigrateFromJSON reads JSON session files from sessionsDir and imports them
// into the given SQLiteStore. Successfully migrated files are renamed to
// .json.migrated (not deleted) as a backup.
//
// Returns the number of sessions successfully migrated.
// Files that are already .migrated or fail to parse are skipped (partial failures
// do not stop the migration of other files).
func MigrateFromJSON(ctx context.Context, sessionsDir string, store *SQLiteStore) (int, error) {
	entries, err := os.ReadDir(sessionsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("memory: read sessions dir: %w", err)
	}

	migrated := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		// Skip already-migrated files.
		if strings.HasSuffix(name, ".migrated") {
			continue
		}

		filePath := filepath.Join(sessionsDir, name)
		data, err := os.ReadFile(filePath)
		if err != nil {
			continue // skip unreadable files
		}

		var sess jsonSession
		if err := json.Unmarshal(data, &sess); err != nil {
			continue // skip invalid JSON
		}

		if sess.Key == "" {
			continue // skip sessions with no key
		}

		if err := importSession(ctx, store, &sess); err != nil {
			continue // skip on import error
		}

		// Rename to .json.migrated as backup.
		_ = os.Rename(filePath, filePath+".migrated")
		migrated++
	}

	return migrated, nil
}

// importSession imports a single JSON session into the SQLite store.
func importSession(ctx context.Context, store *SQLiteStore, sess *jsonSession) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	createdAt := sess.Created.UTC().Format(time.RFC3339)
	updatedAt := sess.Updated.UTC().Format(time.RFC3339)

	// INSERT OR IGNORE to handle partial migration retries.
	_, err = tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO sessions (key, summary, created_at, updated_at) VALUES (?, ?, ?, ?)`,
		sess.Key, sess.Summary, createdAt, updatedAt,
	)
	if err != nil {
		return err
	}

	// Check if messages already exist (idempotent — skip if already imported).
	var count int
	err = tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages WHERE session_key = ?`, sess.Key,
	).Scan(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		// Already imported, nothing to do.
		return tx.Commit()
	}

	now := time.Now().UTC().Format(time.RFC3339)
	for i, msg := range sess.Messages {
		var toolCallsJSON *string
		if len(msg.ToolCalls) > 0 {
			data, err := json.Marshal(msg.ToolCalls)
			if err != nil {
				return err
			}
			s := string(data)
			toolCallsJSON = &s
		}

		_, err := tx.ExecContext(ctx,
			`INSERT INTO messages (session_key, seq, role, content, tool_calls_json, tool_call_id, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			sess.Key, i+1, msg.Role, msg.Content, toolCallsJSON, msg.ToolCallID, now,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
