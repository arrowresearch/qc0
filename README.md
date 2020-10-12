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
with Query Combinators EDSL configured to query PostgreSQL TPC-H database.

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

    { NAME: QUERY... }

This desugars into:

    select(NAME: QUERY...)

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
independently (it's the same composition syntax but because Python syntax
doesn't allow us to reuse `.` as an operator we have `>>` here):

    q.region >> q.name

To apply a query combinator one does:

    q.filter(q.name = q.val('AFRICA'))

Another example where a query combinator is composed with another query:

    q.region.count() # same as q.region >> q.count()

Another example with `select` combinator:

    q.region.select(name=q.name, nation_names=q.nation.name)

To produce queries out of Python value one does:

    q.val(42)
    q.val("Hello")
    q.val(True)
    q.val({"some": ["json"]})

[qc]: https://querycombinators.org/
