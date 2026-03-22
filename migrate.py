"""
migrate.py — one-time database migration
=========================================
Run this ONCE after updating models.py to add the Farm table
and the farm_id column to the Animal table.

    python migrate.py

Safe to run multiple times — each ALTER TABLE is wrapped in a
try/except so it skips columns that already exist.
"""

import sqlite3
import os

DB_PATH = os.getenv("DATABASE_URL", "pasture.db").replace("sqlite:///", "")

def run():
    print(f"Connecting to {DB_PATH} ...")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # ── 1. Create the farm table if it doesn't exist ──────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS farm (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT    NOT NULL,
            street     TEXT    NOT NULL,
            city       TEXT    NOT NULL,
            state      TEXT    NOT NULL,
            created_at TEXT    NOT NULL DEFAULT (datetime('now'))
        )
    """)
    print("✓ farm table ready")

    # ── 2. Add farm_id column to animal if it doesn't exist ───────────────────
    try:
        cur.execute("ALTER TABLE animal ADD COLUMN farm_id INTEGER REFERENCES farm(id)")
        print("✓ animal.farm_id column added")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            print("— animal.farm_id already exists, skipping")
        else:
            raise

    con.commit()
    con.close()
    print("\nMigration complete. Restart uvicorn and reload the dashboard.")

if __name__ == "__main__":
    run()