qc0
===

**WARNING: EXPERIMENTAL, DO NOT USE**

This project attempts to implement a [Query Combinators][qc] to SQL compiler.
The current emphasis is on correctness, generated SQL is not optimal in most but
basic cases.

There's no concrete syntax (yet), the main user interface is a Python API (EDSL)

Interactive Shell
-----------------

There's `qc0-shell` executable provided which gives an interactive Python shell
with a Query Combinators EDSL configured to query a PostgreSQL TPC-H database.

Start interactive shell with:

    % qc0-shell

Then run queries with:

    >>> q.region
    ...   .filter(q.name == q.val('AFRICA'))
    ...   .select(name=q.name, nations=q.nation.name)
    ...   .run()

Syntax
------

Queries are built from syntax constructs. The following syntax is available.

Navigation:

    NAME

Composition:

    QUERY1 . QUERY2

Selection:

    QUERY { FIELD... }

Parametrized queries:

    FUNC(ARG...)

which also allow method-call-like syntax:

    QUERY.FUNC(ARG...) # desugars into FUNC(QUERY, ARG...)

[qc]: https://querycombinators.org/
