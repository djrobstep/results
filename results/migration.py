from logx import log
from migra import Migration
from sqlalchemy.pool import NullPool
from sqlbag import create_database  # noqa
from sqlbag import S, temporary_database

from . import connections


class SchemaManagement:
    def create_database(self, *args, **kwargs):
        create_database(self.db_url, *args, **kwargs)

    def has_same_structure_as(self, other_db_url, **kwargs):
        return db_structures_are_identical(self.db_url, other_db_url, **kwargs)

    def creation_statements(self, **kwargs):
        return creation_statements(self.db_url)

    def drop_statements(self, **kwargs):
        return drop_statements(self.db_url)

    def schema_hash(self):
        return schema_hash(self.db_url)

    def db_differences(self, other_db_url, **kwargs):
        return db_differences(self.db_url, other_db_url, **kwargs)

    def sync_db_structure_to_target_db(
        self, target_db_url, confirm=True, create_extensions_only=False, **kwargs
    ):
        return sync_db_structure_to_target_db(
            self.db_url,
            target_db_url,
            confirm=confirm,
            create_extensions_only=create_extensions_only,
            **kwargs,
        )

    def sync_db_structure_to_setup_method(
        self, setup_method, confirm=True, create_extensions_only=False, **kwargs
    ):
        sync_db_structure_to_setup_method(
            self.db_url,
            setup_method,
            confirm=confirm,
            create_extensions_only=create_extensions_only,
            **kwargs,
        )

    def sync_db_structure_to_definition(
        self, definition_text, confirm=True, create_extensions_only=False, **kwargs
    ):
        sync_db_structure_to_definition(
            self.db_url,
            definition_text,
            confirm=confirm,
            create_extensions_only=create_extensions_only,
            **kwargs,
        )


def prompt(question, prompt=True):  # pragma: no cover
    print(question + " ", end="")
    return input().strip().lower() == "y"


def db_structures_are_identical(db_url_a, db_url_b, **kwargs):
    """
    Do two databases have identical schema structure? This method tells you the answer.

    - db_url_a: first database
    - db_url_b: second database
    - **kwargs: additional args, for passing into a `migra` migration object.

    """
    differences = db_differences(db_url_a, db_url_b, **kwargs)
    return not differences.strip()


def creation_statements(db_url, **kwargs):
    with S(db_url, poolclass=NullPool) as s:
        m = Migration(None, s, **kwargs)
        m.set_safety(False)
        m.add_all_changes()
        return m.sql


def schema_hash(db_url, **kwargs):
    import hashlib

    sql = creation_statements(db_url, **kwargs)
    encoded = sql.encode("utf-8")
    hash = hashlib.sha1(encoded)
    return hash.hexdigest()


def drop_statements(db_url, **kwargs):
    with S(db_url, poolclass=NullPool) as s:
        m = Migration(s, None, **kwargs)
        m.set_safety(False)
        m.add_all_changes()
        return m.sql


def db_differences(db_url_a, db_url_b, **kwargs):
    with S(db_url_a, poolclass=NullPool) as a, S(db_url_b, poolclass=NullPool) as b:
        m = Migration(a, b, **kwargs)
        m.set_safety(False)
        m.add_all_changes()
        return m.sql


def sync_db_structure_to_target_db(
    db_url, target_db_url, confirm=True, create_extensions_only=False, **kwargs
):
    log.info(f"syncing: {db_url} to {target_db_url}")

    with S(db_url, poolclass=NullPool) as s_current, S(
        target_db_url, poolclass=NullPool
    ) as s_target:
        m = Migration(s_current, s_target, **kwargs)
        m.set_safety(False)
        if create_extensions_only:
            log.info("Syncing extension creation only...")
            m.add_extension_changes(creates=True, drops=False)
        else:
            m.add_all_changes()

        if m.statements:
            if confirm:  # pragma: no cover
                print("THE FOLLOWING CHANGES ARE PENDING:", end="\n\n")
                print(m.sql)
                print()
            if not confirm or prompt("Apply these changes?"):
                log.info("Applying...")
                m.apply()
                log.info("Applied.")
            else:
                if confirm:  # pragma: no cover
                    print("Not applying.")
        else:
            if confirm:  # pragma: no cover
                print("Already synced.")

    current_schema_hash = schema_hash(db_url)
    if confirm:  # pragma: no cover
        print(f"Schema hash: {current_schema_hash}")


def sync_db_structure_to_setup_method(
    db_url, setup_method, confirm=True, create_extensions_only=False, **kwargs
):
    with temporary_database(host="localhost") as temp_db_url:
        setup_method(temp_db_url)
        sync_db_structure_to_target_db(
            db_url,
            temp_db_url,
            confirm=confirm,
            create_extensions_only=create_extensions_only,
            **kwargs,
        )


def sync_db_structure_to_definition(
    db_url, definition_text, confirm=True, create_extensions_only=False, **kwargs
):
    def load_def(db_url):
        connections.db(db_url, poolclass=NullPool).raw(definition_text)

    sync_db_structure_to_setup_method(
        db_url,
        load_def,
        confirm=confirm,
        create_extensions_only=create_extensions_only,
        **kwargs,
    )
