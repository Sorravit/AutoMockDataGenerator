import json
import string
import random
from datetime import datetime
import mysql.connector

from src.util.mock_data_util import random_decimal, recommend_value_for_column


class MySqlConnector:
    def __init__(self, host: str, database: str, user: str, password: str, schema: str):
        self.conn = None
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema

    def connect_to_database(self):
        self.conn = mysql.connector.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    def get_table_relationships(self):
        self.connect_to_database()
        cur = self.conn.cursor()
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
        """, (self.schema,))
        tables = cur.fetchall()

        table_relationships = {}
        for table in tables:
            cur.execute("""
                SELECT
                    kcu.table_name,
                    kcu.column_name,
                    kcu.referenced_table_name AS foreign_table_name,
                    kcu.referenced_column_name AS foreign_column_name
                FROM
                    information_schema.key_column_usage AS kcu
                WHERE
                    kcu.referenced_table_name IS NOT NULL
                    AND kcu.table_schema = %s
                    AND kcu.table_name = %s
            """, (self.schema, table[0],))
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
            f"SELECT column_name, data_type, character_maximum_length,NUMERIC_PRECISION,NUMERIC_SCALE FROM information_schema.columns WHERE table_name = '{table_name}'")

        columns = []
        for column in cur.fetchall():
            columns.append({'name': column[0], 'type': column[1], 'max_length': column[2], 'max_digit': column[3], 'max_decimal': column[4]})

        cur.close()

        return columns

    def insert_mock_data(self, table_name, columns_property, dependency=None):
        """
        Insert mock data into the specified table.

        Args:
            table_name (str): The name of the table to insert the data into.
            columns_property (list[dict]): A list of dictionaries representing the columns in the table, where each dictionary has
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
        column_names = [column['name'] for column in columns_property if column['name'] != 'id']
        placeholders = ','.join(['%s'] * len(column_names))
        insert_statement = f"INSERT INTO `{self.schema}`.`{table_name}` (`{'`,`'.join(column_names)}`) VALUES ({placeholders})"

        # Generate mock data
        num_records = 10
        mock_data = []
        for i in range(num_records):
            record = []
            for column_property in columns_property:
                if column_property['name'] in dependency['dependency_columns']:
                    dependency_table_name = dependency['dependencies'][dependency['dependency_columns'].index(column_property['name'])]
                    primary_key_column_name = self.get_primary_key(dependency_table_name)
                    cur.execute(f"SELECT `{primary_key_column_name}` FROM `{dependency_table_name}`")
                    dependency_rows = cur.fetchall()
                    dependency_ids = [row[0] for row in dependency_rows]
                    return random.choice(dependency_ids)
                else:
                    record.append(recommend_value_for_column(column_property))

            mock_data.append(record)

        # Insert the mock data
        print("insert_statement" + insert_statement)
        # print("mock data len  " + str(len(mock_data[0])))
        # print(mock_data)
        cur.executemany(insert_statement, mock_data)
        conn.commit()

        cur.close()

    def get_primary_key(self, table_name):
        # establish the database connection
        self.connect_to_database()
        conn = self.conn
        cur = self.conn.cursor()

        # Execute query to get the primary key of the table
        cur.execute(f"SHOW KEYS FROM `{table_name}` WHERE Key_name = 'PRIMARY'")
        result = cur.fetchone()
        conn.close()

        if result:
            # position 4 seem to be column_name of the primary key
            return result[4]
        else:
            return None
