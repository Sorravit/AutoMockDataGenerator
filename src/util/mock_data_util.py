import json
import string
import random
from datetime import datetime


def random_decimal(max_digits=4, max_decimals=2):
    # calculate the maximum number that can be generated
    max_int_part = 10 ** (max_digits - max_decimals) - 1
    # calculate the maximum value for the decimal part
    max_dec_part = 10 ** max_decimals - 1

    # generate the integer and decimal parts
    int_part = random.randint(0, max_int_part)
    dec_part = random.randint(0, max_dec_part)

    # generate the decimal value
    random_decimal_value = f"{int_part}.{str(dec_part).zfill(max_decimals)}"
    return random_decimal_value


def recommend_value_for_column(column_property, mock_date=datetime.now()):
    if column_property['type'] == 'json':
        json_object = {f'key{n}': f'value{n}' for n in range(random.randint(1, 10))}
        return json.dumps(json_object)
    elif column_property['type'] == 'text':
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    elif column_property['type'] == 'character varying':
        length = random.randint(int(column_property['max_length'] / 2), column_property['max_length'])
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    elif column_property['type'] == 'character':
        length = random.randint(int(column_property['max_length'] / 2), column_property['max_length'])
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    elif column_property['type'] == 'varchar':
        length = random.randint(int(column_property['max_length'] / 2), column_property['max_length'])
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    elif column_property['type'] == 'char':
        length = random.randint(int(column_property['max_length'] / 2), column_property['max_length'])
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    elif column_property['type'] == 'integer':
        return random.randint(1, 100)
    elif column_property['type'] == 'numeric':
        return random.randint(1, 100)
    elif column_property['type'] == 'decimal':
        return random_decimal(column_property['max_digit'], column_property['max_decimal'])
    elif column_property['type'] == 'double precision':
        return random.uniform(1, 100)
    elif column_property['type'] == 'boolean':
        return random.choice([True, False])
    elif column_property['type'] == 'bytea':
        return bytes([random.randint(0, 255) for _ in range(10)])
    elif column_property['type'] == 'timestamp without time zone':
        return mock_date
    elif column_property['type'] == 'int':
        return random.randint(1, 100)
    elif column_property['type'] == 'double':
        return random.uniform(1, 100)
    elif column_property['type'] == 'tinyint':
        return random.choice([1, 0])  # Booleans in MySQL are represented as tinyint
    elif column_property['type'] == 'blob':
        return bytes([random.randint(0, 255) for _ in range(10)])
    elif column_property['type'] == 'datetime':
        return mock_date.strftime('%Y-%m-%d %H:%M:%S')
    else:
        print("Unsupported type " + column_property['type'])


class MockDataUtil:
    pass
