PROMPT_DEFAULT = (
    "Are you sure you wish to apply the statements above to your database?\n"
    "Type 'yes' to apply them, or any other answer to cancel: "
)
MATCHING_MESSAGE = "Schemas match, no changes required.\n"


class Schemas:
    def schemadiff_as_statements(
        self,
        other,
        schema=None,
        exclude_schema=None,
        create_extensions_only=False,
        ignore_extension_versions=False,
        with_privileges=False,
    ):
        from results.dbdiff import Migration

        m = Migration(
            self,
            other,
            schema=schema,
            exclude_schema=exclude_schema,
            ignore_extension_versions=ignore_extension_versions,
        )
        m.set_safety(False)

        if create_extensions_only:
            m.add_extension_changes(drops=False)
        else:
            m.add_all_changes(privileges=with_privileges)
        return m.statements

    def schemadiff_as_sql(self, other, **kwargs):
        statements = self.schemadiff_as_statements(other, **kwargs)
        return statements.sql

    def sync_schema_to_match(
        self,
        other,
        prompt=None,
        matching_message=None,
        tempdb=None,
        raise_on_cancel=True,
        **kwargs,
    ) -> str:
        from .database import Database
        from .tempdb import temporary_local_db

        if isinstance(other, str):
            tempdb = tempdb or temporary_local_db
            with tempdb() as other_db:
                other_db.q(other)
                statements = self.schemadiff_as_statements(other_db, **kwargs)

        elif isinstance(other, Database):
            other_db = other
            statements = self.schemadiff_as_statements(other_db, **kwargs)

        if not statements:
            if matching_message is not False:
                matching_text = matching_message or MATCHING_MESSAGE
                print(matching_text)

        statements_sql = statements.sql

        if prompt is not False:
            print(statements_sql)

            prompt_text = prompt or PROMPT_DEFAULT
            answer = input(prompt_text)

            if answer != "yes":
                print("Not syncing.")
                if raise_on_cancel:
                    raise ValueError('Sync cancelled due to non-"yes" prompt answer.')
                else:
                    return False

        self.q(statements_sql, fail_on_empty=False)
        return statements_sql
