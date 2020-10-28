Using sort() combinator
=======================

``sort()`` combinator sorts input query according to expressions passed as
arguments::

  >>> q.region.sort(q.name.length()).name.run()
  ['ASIA', 'AFRICA', 'EUROPE', 'AMERICA', 'MIDDLE EAST']

  >>> (q.region
  ...  .select(n_customers=q.nation.customer.count(), name=q.name)
  ...  .sort(q.n_customers)
  ...  .run()) # doctest: +NORMALIZE_WHITESPACE
  [{'name': 'EUROPE', 'n_customers': 220},
   {'name': 'AMERICA', 'n_customers': 232},
   {'name': 'AFRICA', 'n_customers': 243},
   {'name': 'MIDDLE EAST', 'n_customers': 252},
   {'name': 'ASIA', 'n_customers': 253}]
