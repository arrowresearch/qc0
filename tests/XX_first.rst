Using first() combinator
========================

``first()`` combinator creates a query which extracts a first element out of the
input query.

Extracting first element from a table::

    >>> q.region.first().run()
    '(AFRICA)'

    >>> q.region.first().name.run()
    'AFRICA'

    >>> q.region.name.first().run()
    'AFRICA'

It is also possible to extract a first element from a plural attribute::

    >>> q.region.select(first_nation=q.nation.name.first()).run()
    [{'first_nation': 'ALGERIA'}]

    >>> q.region.select(first_nation=q.nation.first().name).run()
    [{'first_nation': 'ALGERIA'}]

Let's see how it can be used with ``select()``::

    >>> q.region.first().select(name=q.name).run()
    {'name': 'AFRICA'}

    >>> q.region.select(name=q.name).first().run()
    {'name': 'AFRICA'}

Let's try to some queries with ``filter()``::

    >>> q.region.filter(q.name.substring(1, 1) != 'A').first().name.run()
    'EUROPE'
