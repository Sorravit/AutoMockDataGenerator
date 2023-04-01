# This is a sample Python script.
from src.database_util.database_connector import DatabaseConnector
from src.database_util.database_util import print_database_relations


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
    print_database_relations(table_relation)
