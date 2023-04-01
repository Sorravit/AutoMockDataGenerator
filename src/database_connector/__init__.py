import psycopg2


# Connect to database
def main():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="sorravit",
        password="sorravit"
    )

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Get a list of all tables in the database
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    tables = cur.fetchall()

    # Loop through each table and get its relationships
    for table in tables:
        cur.execute(f"""
            SELECT tc.table_name, kcu.column_name, 
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name 
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='{table[0]}';
        """)
        relationships = cur.fetchall()

        # Print out the table name and its relationships
        print(f"Table: {table[0]}")
        if len(relationships) == 0:
            print("\tNo relationships")
        else:
            for relationship in relationships:
                print(f"\t{relationship[1]} -> {relationship[2]}.{relationship[3]}")

    # Close the cursor and database connection
    cur.close()
    conn.close()
