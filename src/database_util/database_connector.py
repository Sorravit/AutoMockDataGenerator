import psycopg2


class DatabaseConnector:
    def __init__(self, host: str, database: str, user: str, password: str):
        self.conn = None
        self.host = host
        self.database = database
        self.user = user
        self.password = password

    def connect_to_database(self):
        self.conn = psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password
        )

    def get_table_relationships(self):
        self.connect_to_database()
        cur = self.conn.cursor()
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = cur.fetchall()
        table_relationships = {}
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
            table_relationships[table[0]] = [relationship[2] for relationship in relationships]
        cur.close()
        self.conn.close()
        return table_relationships
