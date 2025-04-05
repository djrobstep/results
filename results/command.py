# Copyright results Pty Ltd 2022

import sys

import click

import results


@click.group()
def cli():
    pass


def init_commands(cli):
    @cli.command(help="`diff` two databases, a -> b")
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
    @click.argument("db_url_a", type=str, nargs=1)
    @click.argument("db_url_b", type=str, nargs=1)
    def dbdiff(db_url_a, db_url_b, **kwargs):
        db_a = results.db(db_url_a)
        db_b = results.db(db_url_b)

        schemadiff_sql = db_a.schemadiff_as_sql(db_b, **kwargs)

        if schemadiff_sql:
            print(schemadiff_sql)
            sys.exit(2)


init_commands(cli)
