package memory

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"github.com/sipeed/picoclaw/pkg/providers"
)

// SQLiteStore implements Store backed by a SQLite database.
type SQLiteStore struct {
	db *sql.DB
}

// Open creates or opens a SQLite database at dbPath and returns a ready-to-use SQLiteStore.
func Open(ctx context.Context, dbPath string) (*SQLiteStore, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("memory: create directory: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("memory: open database: %w", err)
	}

	// Single connection — serializes all operations for safety.
	db.SetMaxOpenConns(1)

	// Apply PRAGMAs for embedded-friendly performance.
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA cache_size=-512",
	}
	for _, p := range pragmas {
		if _, err := db.ExecContext(ctx, p); err != nil {
			db.Close()
			return nil, fmt.Errorf("memory: pragma %q: %w", p, err)
		}
	}

	s := &SQLiteStore{db: db}
	if err := s.ensureSchema(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLiteStore) ensureSchema(ctx context.Context) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS sessions (
    key        TEXT PRIMARY KEY,
    summary    TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_key     TEXT    NOT NULL REFERENCES sessions(key) ON DELETE CASCADE,
    seq             INTEGER NOT NULL,
    role            TEXT    NOT NULL,
    content         TEXT    NOT NULL DEFAULT '',
    tool_calls_json TEXT,
    tool_call_id    TEXT    NOT NULL DEFAULT '',
    created_at      TEXT    NOT NULL,
    UNIQUE(session_key, seq)
);`
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("memory: create schema: %w", err)
	}
	return nil
}

// ensureSession inserts a session row if it doesn't already exist.
// Must be called inside a transaction.
func ensureSession(ctx context.Context, tx *sql.Tx, sessionKey string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO sessions (key, summary, created_at, updated_at) VALUES (?, '', ?, ?)`,
		sessionKey, now, now,
	)
	return err
}

// touchSession updates the updated_at timestamp. Must be called inside a transaction.
func touchSession(ctx context.Context, tx *sql.Tx, sessionKey string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := tx.ExecContext(ctx,
		`UPDATE sessions SET updated_at = ? WHERE key = ?`,
		now, sessionKey,
	)
	return err
}

// nextSeq returns the next sequence number for a session. Must be called inside a transaction.
func nextSeq(ctx context.Context, tx *sql.Tx, sessionKey string) (int, error) {
	var maxSeq sql.NullInt64
	err := tx.QueryRowContext(ctx,
		`SELECT MAX(seq) FROM messages WHERE session_key = ?`,
		sessionKey,
	).Scan(&maxSeq)
	if err != nil {
		return 0, err
	}
	if maxSeq.Valid {
		return int(maxSeq.Int64) + 1, nil
	}
	return 1, nil
}

func (s *SQLiteStore) AddMessage(ctx context.Context, sessionKey, role, content string) error {
	return s.AddFullMessage(ctx, sessionKey, providers.Message{
		Role:    role,
		Content: content,
	})
}

func (s *SQLiteStore) AddFullMessage(ctx context.Context, sessionKey string, msg providers.Message) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("memory: begin tx: %w", err)
	}
	defer tx.Rollback()

	err = ensureSession(ctx, tx, sessionKey)
	if err != nil {
		return fmt.Errorf("memory: ensure session: %w", err)
	}

	seq, err := nextSeq(ctx, tx, sessionKey)
	if err != nil {
		return fmt.Errorf("memory: next seq: %w", err)
	}

	var toolCallsJSON *string
	if len(msg.ToolCalls) > 0 {
		var data []byte
		data, err = json.Marshal(msg.ToolCalls)
		if err != nil {
			return fmt.Errorf("memory: marshal tool calls: %w", err)
		}
		str := string(data)
		toolCallsJSON = &str
	}

	now := time.Now().UTC().Format(time.RFC3339)
	_, err = tx.ExecContext(ctx,
		`INSERT INTO messages (session_key, seq, role, content, tool_calls_json, tool_call_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		sessionKey, seq, msg.Role, msg.Content, toolCallsJSON, msg.ToolCallID, now,
	)
	if err != nil {
		return fmt.Errorf("memory: insert message: %w", err)
	}

	err = touchSession(ctx, tx, sessionKey)
	if err != nil {
		return fmt.Errorf("memory: touch session: %w", err)
	}

	return tx.Commit()
}

func (s *SQLiteStore) GetHistory(ctx context.Context, sessionKey string) ([]providers.Message, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT role, content, tool_calls_json, tool_call_id
		 FROM messages
		 WHERE session_key = ?
		 ORDER BY seq ASC`,
		sessionKey,
	)
	if err != nil {
		return nil, fmt.Errorf("memory: query messages: %w", err)
	}
	defer rows.Close()

	var messages []providers.Message
	for rows.Next() {
		var (
			role          string
			content       string
			toolCallsJSON sql.NullString
			toolCallID    string
		)
		if err := rows.Scan(&role, &content, &toolCallsJSON, &toolCallID); err != nil {
			return nil, fmt.Errorf("memory: scan message: %w", err)
		}

		msg := providers.Message{
			Role:       role,
			Content:    content,
			ToolCallID: toolCallID,
		}
		if toolCallsJSON.Valid && toolCallsJSON.String != "" {
			if err := json.Unmarshal([]byte(toolCallsJSON.String), &msg.ToolCalls); err != nil {
				return nil, fmt.Errorf("memory: unmarshal tool calls: %w", err)
			}
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory: rows iteration: %w", err)
	}

	if messages == nil {
		messages = []providers.Message{}
	}
	return messages, nil
}

func (s *SQLiteStore) GetSummary(ctx context.Context, sessionKey string) (string, error) {
	var summary string
	err := s.db.QueryRowContext(ctx,
		`SELECT summary FROM sessions WHERE key = ?`,
		sessionKey,
	).Scan(&summary)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("memory: get summary: %w", err)
	}
	return summary, nil
}

func (s *SQLiteStore) SetSummary(ctx context.Context, sessionKey, summary string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("memory: begin tx: %w", err)
	}
	defer tx.Rollback()

	err = ensureSession(ctx, tx, sessionKey)
	if err != nil {
		return fmt.Errorf("memory: ensure session: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	_, err = tx.ExecContext(ctx,
		`UPDATE sessions SET summary = ?, updated_at = ? WHERE key = ?`,
		summary, now, sessionKey,
	)
	if err != nil {
		return fmt.Errorf("memory: set summary: %w", err)
	}

	return tx.Commit()
}

func (s *SQLiteStore) TruncateHistory(ctx context.Context, sessionKey string, keepLast int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("memory: begin tx: %w", err)
	}
	defer tx.Rollback()

	if keepLast <= 0 {
		_, err = tx.ExecContext(ctx,
			`DELETE FROM messages WHERE session_key = ?`,
			sessionKey,
		)
	} else {
		_, err = tx.ExecContext(ctx,
			`DELETE FROM messages WHERE session_key = ? AND id NOT IN (
				SELECT id FROM messages WHERE session_key = ? ORDER BY seq DESC LIMIT ?
			)`,
			sessionKey, sessionKey, keepLast,
		)
	}
	if err != nil {
		return fmt.Errorf("memory: truncate history: %w", err)
	}

	err = touchSession(ctx, tx, sessionKey)
	if err != nil {
		return fmt.Errorf("memory: touch session: %w", err)
	}

	return tx.Commit()
}

func (s *SQLiteStore) SetHistory(ctx context.Context, sessionKey string, history []providers.Message) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("memory: begin tx: %w", err)
	}
	defer tx.Rollback()

	err = ensureSession(ctx, tx, sessionKey)
	if err != nil {
		return fmt.Errorf("memory: ensure session: %w", err)
	}

	// Delete all existing messages for this session.
	_, err = tx.ExecContext(ctx,
		`DELETE FROM messages WHERE session_key = ?`, sessionKey,
	)
	if err != nil {
		return fmt.Errorf("memory: delete old messages: %w", err)
	}

	// Insert new messages with sequential seq numbers.
	now := time.Now().UTC().Format(time.RFC3339)
	for i, msg := range history {
		var toolCallsJSON *string
		if len(msg.ToolCalls) > 0 {
			var data []byte
			data, err = json.Marshal(msg.ToolCalls)
			if err != nil {
				return fmt.Errorf("memory: marshal tool calls: %w", err)
			}
			str := string(data)
			toolCallsJSON = &str
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO messages (session_key, seq, role, content, tool_calls_json, tool_call_id, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			sessionKey, i+1, msg.Role, msg.Content, toolCallsJSON, msg.ToolCallID, now,
		)
		if err != nil {
			return fmt.Errorf("memory: insert message %d: %w", i, err)
		}
	}

	err = touchSession(ctx, tx, sessionKey)
	if err != nil {
		return fmt.Errorf("memory: touch session: %w", err)
	}

	return tx.Commit()
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}
