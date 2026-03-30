"""One-time script: stamp alembic_version to the correct revision."""

import sqlite3
from pathlib import Path

db = Path(__file__).resolve().parents[1] / "db" / "master.db"
conn = sqlite3.connect(db)
conn.execute("UPDATE alembic_version SET version_num = ?", ("9db879faabd3",))
conn.commit()
rows = conn.execute("SELECT * FROM alembic_version").fetchall()
conn.close()
print("alembic_version now:", rows)
