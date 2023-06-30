"""
Week 1 - Coding Assignment
Specification : Print out table name and their row count
Author : Antara Saha
"""
# Import Libraries
import duckdb

conn = duckdb.connect(database='main_DB_95797') # Connect To DuckDB Database

# To fetch table name
result = conn.execute("SHOW TABLES;").fetchall()
table_list = [row[0] for row in result]

print("Table Name : Row Count")

# To fetch table row count:
for table in table_list:
    sql_command = f"select count(1) from {table};"
    result = conn.execute(sql_command).fetchall()
    row_count = [row[0] for row in result]
    print(table, " : ",row_count)

# End Script