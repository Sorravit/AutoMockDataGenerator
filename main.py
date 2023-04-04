# This is a sample Python script.
from src.database_util.database_connector import DatabaseConnector
from src.database_util.relationship_sorter import get_table_populate_order


# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    database_connector = DatabaseConnector(host="localhost", database="postgres", user="sorravit", password="sorravit")
    table_relation = database_connector.get_table_relationships()

    # Preserve the legacy format
    key_dependency_pairs = dict([(key, value['dependencies']) for key, value in table_relation.items()])
    # print(table_relation)
    # print_database_relations(key_dependency_pairs)
    table_order_list = get_table_populate_order(key_dependency_pairs)

    # print(table_order_list[7])
    # print(table_relation[table_order_list[7]])
    for table_name in table_order_list:
        if table_name != 'flyway_schema_history':
            print("Inserting data to :" + table_name)
            columns = database_connector.get_table_columns(table_name)
            print("columns " + str(columns))
            database_connector.insert_mock_data(table_name, columns, table_relation[table_name])
