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

Python programmers interact with `qc0` by consturcting syntax trees. The main
interface for this is a `q` object.

To produce a **navigation** syntax node one access an attribute of the `q`:

    q.region

To do **composition** one uses `>>` operator:

    q.region >> q.name

To **apply** a query combinator one calls a method on the `q`:

    q.select(name=q.name)

Now when you construct a long query in one shot using `q` with `>>` alone can be
noisy sometimes. For that reason it's also possible to produce **composition**
using `.`:

    q.region.name                 # q.region >> q.name
    q.region.select(name=q.name)  # q.region >> q.select(name=q.name)

There's a special `q.val()` method which allows to build queries from Python
values. Of course such queries always evaluate to constant results designated by
those values passed-in:

    q.val(42)
    q.val("Hello")
    q.val(True)
    q.val({"some": ["json"]})

[qc]: https://querycombinators.org/
