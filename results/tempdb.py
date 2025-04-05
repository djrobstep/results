import random
import time
from contextlib import contextmanager
from string import ascii_lowercase

import results


def random_name(prefix):
    """Generate a random name with prefix, timestamp, and random suffix."""
    time_str = int(time.time())
    random_str = "".join([random.choice(ascii_lowercase) for _ in range(6)])

    return f"{prefix}-{time_str}-{random_str}"


@contextmanager
def temporary_local_db():
    """Create a temporary local database that is automatically cleaned up."""
    db_name = random_name("tempdb-pg").replace("-", "_")

    home_db = results.db()
    temp_db = home_db.create_db(db_name)

    try:
        yield temp_db
    finally:
        home_db.drop_db(db_name, yes_really_drop=True, force=True)
