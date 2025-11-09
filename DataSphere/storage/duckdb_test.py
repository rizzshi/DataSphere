import duckdb

# Connect to DuckDB (file will be created if it doesn't exist)
conn = duckdb.connect('test_duckdb.db')

# Create a test table
conn.execute('CREATE TABLE IF NOT EXISTS test_table (id INTEGER, value VARCHAR)')

# Insert a row
conn.execute('INSERT INTO test_table VALUES (?, ?)', (1, 'hello'))

# Query the table
result = conn.execute('SELECT * FROM test_table').fetchall()
print(f"DuckDB test result: {result}")

conn.close()
