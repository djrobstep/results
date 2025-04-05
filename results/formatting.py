def format_table_of_dicts(table, hierarchical=False):
    """Format a list of dictionaries as a table string."""
    if not table:
        return "Empty table."

    keys = list(table[0].keys())
    types = [str(type(value).__name__) for value in table[0].values()]

    # Convert all values to strings
    string_table = [{k: str(v) for k, v in row.items()} for row in table]

    # Add header row
    string_table.insert(0, {k: k for k in keys})

    # Calculate column widths
    widths = {key: 0 for key in keys}
    for row in string_table:
        for key in keys:
            length = len(row[key])
            if widths[key] < length:
                widths[key] = length

    # Add separator row
    string_table.insert(1, {k: "-" * widths[k] for k in widths})

    # Apply alignment based on type
    for row in string_table:
        for type_name, key in zip(types, keys):
            width = widths[key]
            value = row[key]

            if type_name == "str":
                row[key] = value.ljust(width)
            else:
                row[key] = value.rjust(width)

    # Apply hierarchical formatting if requested
    if hierarchical:
        previous = None
        for row in string_table:
            row_copy = dict(row)

            if previous:
                for key in keys:
                    if row[key] == previous[key]:
                        row[key] = " " * len(row[key])
                    else:
                        break

            previous = row_copy

    return "\n".join(" ".join(row.values()) for row in string_table)
