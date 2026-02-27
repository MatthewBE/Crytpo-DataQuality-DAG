python3 - <<'PY'
import duckdb, os
os.makedirs("warehouse_ddb", exist_ok=True)
conn = duckdb.connect("warehouse_ddb/crypto.duckdb")
conn.execute("INSTALL httpfs;")
conn.execute("LOAD httpfs;")
conn.close()
print("duckdb initialized")
PY