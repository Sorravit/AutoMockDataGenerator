import string
import random
from datetime import datetime

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
            table_relationships[table[0]] = {
                'dependencies': [relationship[2] for relationship in relationships],
                'dependency_columns': [relationship[1] for relationship in relationships]
            }
        cur.close()
        self.conn.close()
        return table_relationships

    def get_table_columns(self, table_name):
        """
        Queries the name and data type of columns within a given table.

        Args:
            table_name (str): The name of the table to query.
            conn (psycopg2.extensions.connection): The database connection object.

        Returns:
            list: A list of dictionaries representing the columns in the table, where each dictionary has keys 'name'
            and 'type'.
            :param table_name:
            :param self:
        """

        self.connect_to_database()
        cur = self.conn.cursor()

        cur.execute(
            f"SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '{table_name}'")

        columns = []
        for column in cur.fetchall():
            columns.append({'name': column[0], 'type': column[1], 'max_length': column[2]})

        cur.close()

        return columns

    def insert_mock_data(self, table_name, columns, dependency=None):
        """
        Insert mock data into the specified table.

        Args:
            table_name (str): The name of the table to insert the data into.
            columns (list[dict]): A list of dictionaries representing the columns in the table, where each dictionary has
            keys 'name' and 'type'.
            dependency (dict): A dictionary representing the dependency table and columns to select from. The dictionary
            has keys 'dependencies' and 'dependency_columns', where 'dependencies' is a list of the names of the
            dependency tables and 'dependency_columns' is a list of the names of the columns in the target table that
            reference the dependency tables.
        """
        # Open a cursor to perform database operations
        self.connect_to_database()
        conn = self.conn
        cur = self.conn.cursor()

        # Generate the insert statement
        column_names = [column['name'] for column in columns if column['name'] != 'id']
        placeholders = ','.join(['%s'] * len(column_names))
        sequence_name = f"{table_name}_seq"
        insert_statement = f"INSERT INTO {table_name} (id,{','.join(column_names)}) VALUES (nextval('{sequence_name}'),{placeholders})"

        # Generate mock data
        num_records = 10
        mock_data = []
        for i in range(num_records):
            record = []
            for column in columns:
                if column['name'] == 'id':
                    continue
                elif column['name'] in dependency['dependency_columns']:
                    dependency_table_name = dependency['dependencies'][
                        dependency['dependency_columns'].index(column['name'])]
                    cur.execute(f"SELECT id FROM {dependency_table_name}")
                    dependency_rows = cur.fetchall()
                    dependency_ids = [row[0] for row in dependency_rows]
                    record.append(random.choice(dependency_ids))
                elif column['type'] == 'text':
                    record.append(''.join(random.choices(string.ascii_uppercase + string.digits, k=5)))
                elif column['type'] == 'character varying':
                    length = random.randint(1, column['max_length'])
                    record.append(''.join(random.choices(string.ascii_uppercase + string.digits, k=length)))
                elif column['type'] == 'integer':
                    record.append(random.randint(1, 100))
                elif column['type'] == 'double precision':
                    record.append(random.uniform(1, 100))
                elif column['type'] == 'boolean':
                    record.append(random.choice([True, False]))
                elif column['type'] == 'bytea':
                    record.append(bytes([random.randint(0, 255) for _ in range(10)]))
                elif column['type'] == 'timestamp without time zone':
                    record.append(datetime.now())
            mock_data.append(record)

        # Insert the mock data
        print(insert_statement)
        print(mock_data)
        cur.executemany(insert_statement, mock_data)
        conn.commit()

        cur.close()
