"""
Week 1 - Coding Assignment
Specification : Print out table names and schemas
Author : Antara Saha
"""
# Import Libraries
import duckdb

conn = duckdb.connect(database='main_DB_95797') # Connect To DuckDB Database

# To fetch table name
result = conn.execute("SHOW TABLES;").fetchall()

table_list = [row[0] for row in result]
formatted_output = '\n'.join(table_list)

print("Tables Listed in DuckDB Database are as follows:")
print(formatted_output,"\n")

# To fetch Schemas 
for table in table_list:
    sql_command = f"DESCRIBE {table};"
    result = conn.execute(sql_command).fetchall()
    print("Schema Details for table:",table)
    for row in result:
        print(row)
    print("\n")

# End Script