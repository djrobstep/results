# Don't get mad, get `results`

`results` is a database library with a focus on postgres. 

It does a bunch of different things, but the part you are most likely interested in is the database diffing functionality, which I originally built as `migra`, and whose legend now continues in this project.

Most of this library is in alpha state. I'll continue to change it willy-nilly and move things around with not a care in the world. However the db diffing part I'll aim to keep relatively stable, except between major versions, given it has already reached a certain level of maturity.

Some slightly better docs forthcoming, but for now, here's a brief rundown, scroll to the bottom for the `migra` part.


## Contributing

Before filing issues or creating pull requests, please remember that this is an entirely spare time project, please don't create PRs without some discussion first, please don't make comments along the lines of "it's really important that you fix this immediately because my large company depends on it".

I got a bit jaded first time around from working on `migra`. I intend to avoid that this time around.

By the way, I haven't uploaded the tests yet - I need to clean them up. I'm sure I'll get around to it at some point.


## The basics

Create a database object as follows, with a name or connection url.

```python
import results

db = results.db('example')
db = results.db('postgres:///example')
```

Basic queries: 

```python
result = db.q('select :x', dict(x=5))
```

Transactions:

```python
with db.t() as t:
    t.q('select 1')
```

Upserting:

```python
db.insert("table", dict(a="hi", b=1), upsert_on="a")
```

Paging (offset-free!):

```python
bookmark = None

while True:
    page = db.q(
        "select * from names",
        paging=dict(order_by="name desc, id", per_page=2, bookmark=bookmark),
    )

    print(page)

    if not page.paging.has_next:
        break

    bookmark = page.paging.next
```

Notifications/pubsub:

```python
with db.listening_connection(["hello", "hello2"]) as listen_conn:
        for n in db.notifications(
            listen_conn
        ):
            print(n)
```

## Schema diffing/migration generation:

Suppose we have two databases, `a` and `b`, and we would like `a` to have the structure of `b`. Common problem - after all, this is why migration frameworks exist.

The "diff" of these databases is the SQL script that transforms A into B structurally.

Here's how you can use `results` to make such a diff.

```python
from results import db

diff = db('a').schemadiff_as_sql(db('b'))
print(diff)
```

(Remember, this only handles structure, and won't handle any necessary data changes like renames, moving data from a column before it gets deleted, etc etc)


### The `migra` command is now `results dbdiff`

The command line version should work much the same as `migra` did, with a couple of small differences/enhancements.

To get the schema diff from database `a` to database `b` (formerly `migra postgresql:///a postgresql:///b`):

    results dbdiff postgresql:///a postgresql:///b

One difference is that you can now just specify a database name (rather than full connection url) for local dbs. So the above can just be:

    results dbdiff a b

`migra` also used to fail if any drop statements were generated unless you added the `--unsafe` flag. I've decided that was more trouble than it was worth, so I've removed it. 

So please be careful! `results dbdiff` only generates scripts, but those scripts could delete your entire database if applied.

The various other options remain, as per the `results dbdiff --help`:

```
Options:
  --schema TEXT                Restrict output to single schema
  --exclude-schema TEXT        Restrict output to statements for all schemas
                               except the specified schema
  --create-extensions-only     Only output "create extension..." statements,
                               nothing else
  --ignore-extension-versions  Ignore the versions when comparing extensions
  --with-privileges            Also output privilege differences (ie.
                               grant/revoke statements)
  --help                       Show this message and exit.
```




