import pendulum
from pendulum.date import Date as date_type
from pendulum.datetime import DateTime as datetime_type

# Mapping from Python types to SQL types
PY_TO_SQL = {
    str: "text",
    datetime_type: "timestamptz",
    date_type: "date",
    float: "decimal",
    int: "bigint",
    None: "text",
}


def guess_value_type(value):
    """Guess the Python type of a single value."""
    if value == "":
        return None

    # Try to parse as number
    try:
        float_value = float(value)
        if float_value.is_integer():
            return int
        else:
            return float
    except (ValueError, TypeError):
        pass

    # Try to parse as date/datetime
    try:
        parsed = pendulum.parse(value, exact=True)

        if isinstance(parsed, datetime_type):
            return datetime_type

        if isinstance(parsed, date_type):
            return date_type

    except Exception:
        pass

    return str


def guess_type_of_values(values):
    """Guess the most appropriate type for a collection of values."""
    present_types = {guess_value_type(x) for x in values if x is not None}

    # Return most specific type that can handle all values
    if str in present_types:
        return str
    if datetime_type in present_types:
        return datetime_type
    if date_type in present_types:
        return date_type
    if float in present_types:
        return float
    if int in present_types:
        return int
    return str


def guess_sql_type_of_values(values):
    """Guess the appropriate SQL type for a collection of values."""
    return PY_TO_SQL[guess_type_of_values(values)]
