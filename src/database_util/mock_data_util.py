import random


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


class MockDataUtil:
    pass
