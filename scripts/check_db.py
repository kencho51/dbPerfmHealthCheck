import sqlite3
con = sqlite3.connect("db/master.db")
tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
print("Tables in master.db:")
for t in tables:
    print(f"  {t}")
ver = con.execute("SELECT version_num FROM alembic_version").fetchone()
print(f"\nAlembic version: {ver[0] if ver else 'none'}")

