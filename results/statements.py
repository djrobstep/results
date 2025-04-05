from textwrap import indent


def create_table_statement(name, columns, extra=None, *, uuid_pk=None):
    """Generate a CREATE TABLE SQL statement."""
    extra = extra or {}

    # Build column specifications
    colspec = [
        dict(name=col_name, coltype=col_type, extra=extra.get(col_name))
        for col_name, col_type in columns.items()
    ]

    # Add UUID primary key if requested
    if uuid_pk:
        uuid_col = dict(
            name=uuid_pk,
            coltype="uuid",
            extra="default gen_random_uuid() primary key",
        )
        colspec = [uuid_col] + colspec

    def format_column_line(column_dict):
        """Format a single column definition line."""
        line = f"{column_dict['name']} {column_dict['coltype']}"

        if column_extra := column_dict.get("extra"):
            line += f" {column_extra}"

        return line

    column_lines = [format_column_line(col) for col in colspec]
    column_text = ",\n".join(column_lines)
    column_text = indent(column_text, "  ")

    statement = f"""\
create table {name} (
{column_text}
);

"""
    return statement
