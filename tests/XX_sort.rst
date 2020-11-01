Using sort() combinator
=======================

``sort()`` combinator sorts input query according to expressions passed as
arguments::

  >>> q.region.sort(q.name.length()).name.run()
  ['ASIA', 'AFRICA', 'EUROPE', 'AMERICA', 'MIDDLE EAST']

  >>> (q.region
  ...  .select(num_customers=q.nation.customer.count(), name=q.name)
  ...  .sort(q.num_customers)
  ...  .run()) # doctest: +NORMALIZE_WHITESPACE
  [{'name': 'EUROPE', 'num_customers': 220},
   {'name': 'AMERICA', 'num_customers': 232},
   {'name': 'AFRICA', 'num_customers': 243},
   {'name': 'MIDDLE EAST', 'num_customers': 252},
   {'name': 'ASIA', 'num_customers': 253}]
