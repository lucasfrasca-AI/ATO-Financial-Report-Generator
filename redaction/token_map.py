"""
token_map.py — SQLite-backed token store for the redaction layer.

Each detected PII entity is assigned a deterministic token (e.g. AU_TFN_001).
The mapping is stored in data/session.db and can be exported to JSON.

Schema:
  tokens(id INTEGER PK, entity_type TEXT, token TEXT UNIQUE, original TEXT, confidence REAL, source TEXT)
  run_log(id INTEGER PK, ts TEXT, level TEXT, message TEXT)
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


class TokenMap:
    """Session-scoped SQLite token store."""

    def __init__(self, db_path: str = "./data/session.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _init_schema(self):
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS tokens (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT    NOT NULL,
                token       TEXT    NOT NULL UNIQUE,
                original    TEXT    NOT NULL,
                confidence  REAL    NOT NULL,
                source      TEXT    NOT NULL DEFAULT '',
                created_at  TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS run_log (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                ts      TEXT    NOT NULL,
                level   TEXT    NOT NULL,
                message TEXT    NOT NULL
            );
        """)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Token operations
    # ------------------------------------------------------------------

    def get_or_create_token(
        self,
        entity_type: str,
        original: str,
        confidence: float,
        source: str = "",
    ) -> str:
        """
        Return an existing token for `original` (same entity_type) or create a new one.
        Token format: {ENTITY_TYPE}_{NNN} — three-digit zero-padded counter per type.
        """
        row = self._conn.execute(
            "SELECT token FROM tokens WHERE entity_type = ? AND original = ?",
            (entity_type, original),
        ).fetchone()
        if row:
            return row["token"]

        count_row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM tokens WHERE entity_type = ?",
            (entity_type,),
        ).fetchone()
        seq = (count_row["n"] if count_row else 0) + 1
        token = f"{entity_type}_{seq:03d}"

        self._conn.execute(
            """INSERT INTO tokens (entity_type, token, original, confidence, source, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (entity_type, token, original, confidence, source, _now()),
        )
        self._conn.commit()
        logger.debug("New token %s -> %r (conf=%.2f)", token, original, confidence)
        return token

    def resolve_token(self, token: str):
        """Return the original value for a token, or None if not found."""
        row = self._conn.execute(
            "SELECT original FROM tokens WHERE token = ?", (token,)
        ).fetchone()
        return row["original"] if row else None

    def all_tokens(self):
        """Return all token records as a list of dicts."""
        rows = self._conn.execute(
            "SELECT entity_type, token, original, confidence, source, created_at FROM tokens ORDER BY id"
        ).fetchall()
        return [dict(r) for r in rows]

    def tokens_by_type(self):
        """Return {entity_type: [records]} grouping."""
        result = {}
        for rec in self.all_tokens():
            result.setdefault(rec["entity_type"], []).append(rec)
        return result

    # ------------------------------------------------------------------
    # Run log
    # ------------------------------------------------------------------

    def log(self, level: str, message: str):
        self._conn.execute(
            "INSERT INTO run_log (ts, level, message) VALUES (?, ?, ?)",
            (_now(), level.upper(), message),
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export_json(self, path: str):
        """Write full token map to a JSON file."""
        data = {
            "exported_at": _now(),
            "tokens": self.all_tokens(),
        }
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(data, indent=2), encoding="utf-8")
        logger.info("Token map exported to %s", path)

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
