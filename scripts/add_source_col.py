import sqlite3
c = sqlite3.connect("db/master.db")
cols = [r[1] for r in c.execute("PRAGMA table_info(pattern_label)").fetchall()]
if "source" not in cols:
    c.execute("ALTER TABLE pattern_label ADD COLUMN source TEXT NOT NULL DEFAULT 'both'")
    c.commit()
    print("Column 'source' added with default 'both'")
else:
    print("Column already exists, skipping")
cols2 = [r[1] for r in c.execute("PRAGMA table_info(pattern_label)").fetchall()]
print("Columns:", cols2)
