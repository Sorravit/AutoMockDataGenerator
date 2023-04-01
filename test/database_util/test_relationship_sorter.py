import unittest

from src.database_util.relationship_sorter import get_table_populate_order


class TestTablePopulateOrder(unittest.TestCase):

    def test_get_table_populate_order(self):
        table_relationships = {
            'flyway_schema_history': [],
            'brand_group': [],
            'product_brand': ['brand_group'],
            'waste_group': [],
            'waste_type': ['waste_group'],
            'waste_sub_type': ['waste_group'],
            'sorting_collection': ['product_brand', 'color', 'waste_sub_type'],
            'color': [],
            'location': [],
            'batch': ['location', 'batch_type'],
            'batch_type': [],
            'sorting_sub_collection': ['sorting_collection'],
            'collector_collection': ['batch'],
            'collector_sub_collection': ['collector_collection'],
            'basket': ['location'],
            'sorter_collection': ['batch', 'waste_sub_type'],
            'sorter_sub_collection': ['sorter_collection'],
            'sorting_collection_junction_table': ['sorting_collection', 'batch'],
        }

        table_order = get_table_populate_order(self.table_relationships)
        expected_order = ['waste_group',
                          'brand_group',
                          'batch_type',
                          'location',
                          'waste_sub_type',
                          'color',
                          'product_brand',
                          'batch',
                          'sorting_collection',
                          'sorter_collection',
                          'collector_collection',
                          'sorting_collection_junction_table',
                          'sorter_sub_collection',
                          'basket',
                          'collector_sub_collection',
                          'sorting_sub_collection',
                          'waste_type',
                          'flyway_schema_history']
        self.assertEqual(expected_order, table_order)

    def test_simple_graph_with_one_correct_answer(self):
        table_relationships = {
            'A': ['B', 'C'],
            'B': ['D'],
            'C': ['D', 'B'],
            'D': [],
        }
        expected_result = ['D', 'B', 'C', 'A']
        actual_result = get_table_populate_order(table_relationships)
        self.assertEqual(expected_result, actual_result)

    def test_missing_dependency_table(self):
        table_relationships = {
            'A': ['B'],
            'B': ['C'],
            'C': ['D'],
            'D': ['E']
        }
        with self.assertRaises(KeyError):
            get_table_populate_order(table_relationships)

    def test_cycle(self):
        table_relationships = {
            'table1': ['table2'],
            'table2': ['table3'],
            'table3': ['table1']
        }

        with self.assertRaises(ValueError):
            get_table_populate_order(table_relationships)
