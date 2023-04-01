import unittest
from unittest.mock import patch
from io import StringIO

from src.database_util.database_util import print_database_relations


class TestDatabaseUtil(unittest.TestCase):
    @patch('sys.stdout', new_callable=StringIO)
    def test_print_database_relations_with_no_dependencies(self, mock_stdout):
        table_relationships = {
            'table1': [],
            'table2': [],
            'table3': []
        }
        print_database_relations(table_relationships)
        expected_output = 'Table: table1\n\tNo dependencies\nTable: table2\n\tNo dependencies\nTable: table3\n\tNo ' \
                          'dependencies\n'
        self.assertEqual(mock_stdout.getvalue(), expected_output)

    @patch('sys.stdout', new_callable=StringIO)
    def test_print_database_relations_with_dependencies(self, mock_stdout):
        table_relationships = {
            'table1': ['table2', 'table3'],
            'table2': ['table3'],
            'table3': []
        }
        print_database_relations(table_relationships)
        expected_output = 'Table: table1\n\tDepends on: table2\n\tDepends on: table3\nTable: table2\n\tDepends on: ' \
                          'table3\nTable: table3\n\tNo dependencies\n'
        self.assertEqual(mock_stdout.getvalue(), expected_output)
