from typing import Dict, List
from collections import deque


def get_table_populate_order(table_relationships: Dict[str, List[str]]) -> List[str]:
    """
    Determines the order in which to populate tables based on their relationships.

    Args:
        table_relationships (dict): A dictionary of table relationships, where each key is a table name and the
            corresponding value is a list of table names that the key table depends on.

    Returns:
        list: A list of table names in the order in which they should be populated.
    """
    table_order = []
    table_queue = deque()

    # Create a dictionary to keep track of how many dependencies each table has
    table_dependency_counts = {table: 0 for table in table_relationships}

    # Calculate the dependency counts for each table
    for table, dependencies in table_relationships.items():
        for dependency in dependencies:
            table_dependency_counts[dependency] += 1

    # Add tables with no dependencies to the queue
    for table, count in table_dependency_counts.items():
        if count == 0:
            table_queue.append(table)

    # Process the tables in the queue
    while table_queue:
        table = table_queue.popleft()

        # Append the table to the table order list
        table_order.append(table)

        # Decrement the dependency count for each dependent table
        for dependent_table in table_relationships[table]:
            table_dependency_counts[dependent_table] -= 1

            # Add the dependent table to the queue if all its dependencies have been processed
            if table_dependency_counts[dependent_table] == 0:
                table_queue.append(dependent_table)

    # Check if all tables have been processed
    if len(table_order) != len(table_relationships):
        raise ValueError("The table relationships are incomplete or contain a cycle")

    # Reverse the table order list before returning it
    return table_order[::-1]
