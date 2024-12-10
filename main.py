# This is a sample Python script.
from src.database_util.mysql_connector import MySqlConnector
from src.database_util.postgres_connector import PostgresConnector
from src.util.relationship_sorter import get_table_populate_order


# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    # database_connector = PostgresConnector(host="localhost", database="postgres", user="postgres", password="sorravit", schema='public')
    database_connector = MySqlConnector(host="localhost", database="etaxportal", user="root", password="sorravit",schema='etaxportal')
    table_relation = database_connector.get_table_relationships()

    # Preserve the legacy format
    key_dependency_pairs = dict([(key, value['dependencies']) for key, value in table_relation.items()])
    # print(table_relation)
    # print_database_relations(key_dependency_pairs)
    table_order_list = get_table_populate_order(key_dependency_pairs)

    # tableOrder = 0
    # print(table_order_list[tableOrder])
    # print(table_relation[table_order_list[tableOrder]])
    # print("get_primary_key: " + database_connector.get_primary_key(table_order_list[tableOrder]))
    # print(table_order_list)
    for table_name in table_order_list:
        if table_name != 'flyway_schema_history':
            print("Inserting data to :" + table_name)
            # print("get_primary_key: " + database_connector.get_primary_key(table_name))
            columns = database_connector.get_table_columns(table_name)
            print("columns " + str(columns))
            database_connector.insert_mock_data(table_name, columns, table_relation[table_name])
