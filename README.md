qc0
===

**WARNING: EXPERIMENTAL, DO NOT USE**

This project attempts to implement a [Query Combinators][qc] to SQL compiler.
The current emphasis is on correctness, generated SQL is not optimal in most but
basic cases.

There's no concrete syntax (yet), the main user interface is a Python API (EDSL)

Trying it out
-------------

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

While there's no concrete syntax yet, we still describe it here. Note that it
differs slightly from the Python EDSL API (introduced in the next section) due
to the nature of the embedding into Python.

**Navigation** queries data specified by the identifier `NAME`:

    NAME

**Composition** of two queries `QUERY1` and `QUERY2`, it performs `QUERY1` first and
then performs `QUERY2` in its resulting context:

    QUERY1 . QUERY2

**Applicaton** applies a query combinator to its argument queries `QUERY`:

    COMB(QUERY...)

Syntax Sugar
------------

The ``select`` query combinator is so common that there's **selection** syntax
sugar available:

    QUERY_PARENT { NAME: QUERY... }

This desugars into:

    select(QUERY_PARENT, NAME: QUERY...)

Common query combinators like `filter`, `sort`, `take`, `count` (and many
others) operate on a "primary query" (query which is used as a basis for
filtering, sorting, ...). For this case there's a **method-call** syntax sugar:

    QUERY_ARG.COMB(QUERY_EXTRA_ARG...)

This desugars into:

    COMB(QUERY_ARG, QUERY_EXTRA_ARG...)

Python EDSL API
---------------

There's `q` object provided which one can use to build queries by accessing its
attributes and calling its methods. Below is a description on how to produce
syntax constructs (described above) using `q`.

Navigation:

    q.name

As `q` implements "Builder Pattern" one can use build queries out of previously
built queries. For example to navigate to `region` and then to `name` looks
naturally like:

    q.region.name

Another syntax for composition enables to compose two queries built
independently:

    q.region >> q.name

To apply a query combinator one does:

    q.count(q.region)

Alternatively a query combinator can be used as a method (in this case it
receives the left hand side query as its first argument):

    q.region.count()

Another example with `select` combinator:

    q.region.select(name=q.name, nation_names=q.nation.name)

To produce queries out of Python value one does:

    q.val(42)
    q.val("Hello")
    q.val(True)
    q.val({"some": ["json"]})

[qc]: https://querycombinators.org/
