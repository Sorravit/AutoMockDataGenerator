def print_database_relations(table_relationships):
    """
    Prints the relationships between tables in the database.

    Args:
        table_relationships (dict): A dictionary of table relationships, where each key is a table name and the
        corresponding value is a list of table names that the key table depends on.
    """
    for table, dependencies in table_relationships.items():
        print(f"Table: {table}")
        if len(dependencies) == 0:
            print("\tNo dependencies")
        else:
            for dependency in dependencies:
                print(f"\tDepends on: {dependency}")
