# Copyright results Pty Ltd 2022

import sys

import click

import results


@click.group()
def cli():
    pass


def init_commands(cli):
    @cli.command(
        help=(
            "Analyse what locks each statement in SQL_FILE acquires.\n\n"
            "DB_URL is the database to copy schema from, or the special value "
            "EMPTY to use a blank database. The script is never applied to the "
            "real database — a temporary copy is created and dropped automatically."
        )
    )
    @click.argument("sql_file", type=click.Path(exists=True, readable=True))
    @click.argument("db_url", type=str, default="EMPTY")
    @click.option("--verbose", "-v", is_flag=True, help="Show lock descriptions")
    def lockinfo(sql_file, db_url, verbose):
        import results
        from results.lockinfo import analyse_locks, format_results
        from results.tempdb import temporary_local_db

        sql = click.open_file(sql_file).read()

        if db_url == "EMPTY":
            source_url = None
        else:
            source_url = db_url

        with temporary_local_db(source_url) as temp_db:
            if source_url:
                # Copy schema from source into the temp db
                source_db = results.db(source_url)
                schema_sql = source_db.schemadiff_as_sql(temp_db)
                if schema_sql:
                    temp_db.q(schema_sql, fail_on_empty=False)

            lock_results = analyse_locks(temp_db.url, sql)

        output = format_results(lock_results, verbose=verbose)
        click.echo(output)

        # Exit 1 if any notable locks were found, 2 if any statements errored
        errors = [r for r in lock_results if r.error]
        notable = [r for r in lock_results if r.notable_locks]
        if errors:
            raise SystemExit(2)
        if notable:
            raise SystemExit(1)

    @cli.command(help="`diff` two schemas, a -> b. Each argument can be a DB URL or a definition file (.yaml/.json).")
    @click.option("--schema", help="Restrict output to single schema", default=None)
    @click.option(
        "--exclude-schema",
        help="Restrict output to statements for all schemas except the specified schema",
        default=None,
    )
    @click.option(
        "--create-extensions-only",
        is_flag=True,
        help='Only output "create extension..." statements, nothing else',
    )
    @click.option(
        "--ignore-extension-versions",
        is_flag=True,
        help="Ignore the versions when comparing extensions",
    )
    @click.option(
        "--with-privileges",
        is_flag=True,
        default=False,
        help="Also output privilege differences (ie. grant/revoke statements)",
    )
    @click.argument("a", type=str, nargs=1)
    @click.argument("b", type=str, nargs=1)
    def dbdiff(a, b, **kwargs):
        from results.schemainspect import SchemaDefinition

        def load(arg):
            import os
            if os.path.isfile(arg):
                text = open(arg).read()
                if arg.endswith(".json"):
                    return SchemaDefinition.from_json(text)
                else:
                    return SchemaDefinition.from_yaml(text)
            return results.db(arg)

        source_a = load(a)
        source_b = load(b)

        schemadiff_sql = source_a.schemadiff_as_sql(source_b, **kwargs)

        if schemadiff_sql:
            print(schemadiff_sql)
            sys.exit(2)


init_commands(cli)
