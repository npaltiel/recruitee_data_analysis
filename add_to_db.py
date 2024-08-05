def insert_or_replace(table_name, df, conn):
    # Get the columns of the DataFrame
    columns = ", ".join(df.columns)
    # Create placeholders for the values
    placeholders = ", ".join(["?"] * len(df.columns))
    # Create the SQL statement
    sql = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"
    # Execute the SQL statement for each row in the DataFrame
    cur = conn.cursor()
    for row in df.itertuples(index=False, name=None):
        cur.execute(sql, row)
    conn.commit()