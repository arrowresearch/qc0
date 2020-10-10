qc0
===

**WARNING: EXPERIMENTAL, DO NOT USE**

This project attempts to implement a [Query Combinators][qc] to SQL compiler.
The current emphasis is on correctness, generated SQL is not optimal in most but
basic cases.

There's no concrete syntax (yet), the main user interface is a Python API (EDSL)

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
