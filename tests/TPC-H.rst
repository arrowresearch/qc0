TPC-H
=====

Pricing Summary Report Query (Q1)
---------------------------------

SQL::

  >>> expected = execute_sql("""
  ... select
  ...   l.returnflag,
  ...   l.linestatus,
  ...   sum(l.quantity) as sum_qty,
  ...   sum(l.extendedprice) as sum_base_price,
  ...   sum(l.extendedprice*(1-l.discount)) as sum_disc_price,
  ...   sum(l.extendedprice*(1-l.discount)*(1+l.tax)) as sum_charge,
  ...   avg(l.quantity) as avg_qty,
  ...   avg(l.extendedprice) as avg_price,
  ...   avg(l.discount) as avg_disc,
  ...   count(*) as count_order
  ... from lineitem l
  ... where
  ...   l.shipdate <= date '1998-12-01'
  ... group by
  ...   l.returnflag,
  ...   l.linestatus
  ... order by
  ...   l.returnflag,
  ...   l.linestatus;
  ... """)

qc0::

  >>> got = (q.lineitem
  ...  .filter(q.shipdate < date(1998, 12, 1))
  ...  .group(returnflag=q.returnflag, linestatus=q.linestatus)
  ...  .select(
  ...    returnflag=q.returnflag,
  ...    linestatus=q.linestatus,
  ...    sum_qty=q._.quantity.sum(),
  ...    sum_base_price=q._.extendedprice.sum(),
  ...    sum_disc_price=q._ >> (q.extendedprice * (1 - q.discount)) >> q.sum(),
  ...    sum_charge=q._ >> (q.extendedprice * (1 - q.discount) * (1 + q.tax)) >> q.sum(),
  ...    avg_qty=q._.quantity.avg(),
  ...    avg_price=q._.extendedprice.avg(),
  ...    avg_disc=q._.discount.avg(),
  ...    count_order=q._.count(),
  ...  )
  ...  .sort(q.returnflag, q.linestatus)
  ...  .run())

  >>> got == expected
  True

  >>> got # doctest: +NORMALIZE_WHITESPACE
  [{'avg_disc': 0.05011958521491888,
    'avg_price': 33577.13907007861,
    'avg_qty': 25.40316106372303,
    'count_order': 11958,
    'linestatus': 'F',
    'returnflag': 'A',
    'sum_base_price': 401515429.0,
    'sum_charge': 396851570.111263,
    'sum_disc_price': 381465877.1614,
    'sum_qty': 303771},
   {'avg_disc': 0.04809027777777778,
    'avg_price': 33754.776875,
    'avg_qty': 25.53472222222222,
    'count_order': 288,
    'linestatus': 'F',
    'returnflag': 'N',
    'sum_base_price': 9721375.74,
    'sum_charge': 9616674.437564,
    'sum_disc_price': 9247800.679,
    'sum_qty': 7354},
   {'avg_disc': 0.049931680899812536,
    'avg_price': 33790.326112059985,
    'avg_qty': 25.458529473026452,
    'count_order': 24005,
    'linestatus': 'O',
    'returnflag': 'N',
    'sum_base_price': 811136778.32,
    'sum_charge': 801746504.456339,
    'sum_disc_price': 770866712.5807,
    'sum_qty': 611132},
   {'avg_disc': 0.04973083674663546,
    'avg_price': 33940.52955028003,
    'avg_qty': 25.630945415029675,
    'count_order': 11963,
    'linestatus': 'F',
    'returnflag': 'R',
    'sum_base_price': 406030555.01,
    'sum_charge': 401431970.03021,
    'sum_disc_price': 385792652.5409,
    'sum_qty': 306623}]


Minimum Cost Supplier Query (Q2)
--------------------------------

SQL::

  >>> expected = execute_sql("""
  ... select
  ...   s.acctbal as s_acctbal,
  ...   s.name as s_name,
  ...   n.name as n_name,
  ...   p.name as p_name,
  ...   p.mfgr as p_mfgr,
  ...   s.address as s_address,
  ...   s.phone as s_phone,
  ...   s.comment as s_comment
  ... from
  ...   part p,
  ...   supplier s,
  ...   partsupp ps,
  ...   nation n,
  ...   region r
  ... where
  ...   p.id = ps.part_id
  ...   and s.id = ps.supplier_id
  ...   and p.size = 45
  ...   and p.type like '%%NICKEL'
  ...   and s.nation_id = n.id
  ...   and n.region_id = r.id
  ...   and r.name = 'EUROPE'
  ...   and ps.supplycost = (
  ...     select 
  ...       min(ps.supplycost)
  ...     from
  ...       partsupp ps, supplier s,
  ...       nation n, region r
  ...     where
  ...       p.id = ps.part_id
  ...       and s.id = ps.supplier_id
  ...       and s.nation_id = n.id
  ...       and n.region_id = r.id
  ...       and r.name = 'EUROPE'
  ...   )
  ... order by
  ...   s.acctbal desc,
  ...   n.name,
  ...   s.name,
  ...   p.name
  ... """)

::

  >>> got = (q.partsupp
  ...  .filter(
  ...     (q.supplier.nation.region.name == 'EUROPE') &
  ...     q.part.type.like('%NICKEL') &
  ...     (q.part.size == 45)
  ...  )
  ...  .filter(q.supplycost == q.around(q.part).supplycost.min())
  ...  .select(
  ...     s_acctbal=q.supplier.acctbal,
  ...     s_name=q.supplier.name,
  ...     n_name=q.supplier.nation.name,
  ...     p_name=q.part.name,
  ...     p_mfgr=q.part.mfgr,
  ...     s_address=q.supplier.address,
  ...     s_phone=q.supplier.phone,
  ...     s_comment=q.supplier.comment,
  ...  )
  ...  .sort(
  ...     q.s_acctbal.desc(),
  ...     q.n_name,
  ...     q.s_name,
  ...     q.p_name
  ...  )
  ...  .run()) # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS

  >>> got == expected
  True

  >>> got # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
  [{'n_name': 'ROMANIA',
    'p_mfgr': 'Manufacturer#1',
    'p_name': 'olive purple turquoise cornflower honeydew',
    's_acctbal': 9202.57,
    's_address': 'bSmlFYUKBeRsqJxwC9 zS6xpFdEf5jNTb',
    's_comment': '...',
    's_name': 'Supplier#000000062',
    's_phone': '29-603-653-2494'},
   {'n_name': 'ROMANIA',
    'p_mfgr': 'Manufacturer#1',
    'p_name': 'pink powder mint moccasin navajo',
    's_acctbal': 9202.57,
    's_address': 'bSmlFYUKBeRsqJxwC9 zS6xpFdEf5jNTb',
    's_comment': '...',
    's_name': 'Supplier#000000062',
    's_phone': '29-603-653-2494'},
   {'n_name': 'ROMANIA',
    'p_mfgr': 'Manufacturer#4',
    'p_name': 'thistle sky antique khaki chartreuse',
    's_acctbal': 9202.57,
    's_address': 'bSmlFYUKBeRsqJxwC9 zS6xpFdEf5jNTb',
    's_comment': '...',
    's_name': 'Supplier#000000062',
    's_phone': '29-603-653-2494'},
   {'n_name': 'RUSSIA',
    'p_mfgr': 'Manufacturer#1',
    'p_name': 'spring wheat purple chiffon puff',
    's_acctbal': 9198.31,
    's_address': 'RCQKONXMFnrodzz6w7fObFVV6CUm2q',
    's_comment': '...',
    's_name': 'Supplier#000000025',
    's_phone': '32-431-945-3541'}]

Shipping Priority Query (Q3)
----------------------------

SQL::

  >>> expected = execute_sql("""
  ... select
  ...   l.order_id,
  ...   sum(l.extendedprice * (1 - l.discount)) as revenue,
  ...   o.orderdate,
  ...   o.shippriority
  ... from
  ...   customer c,
  ...   "order" o,
  ...   lineitem l
  ... where
  ...   c.mktsegment = 'BUILDING'
  ...   and c.id = o.customer_id
  ...   and l.order_id = o.id
  ...   and o.orderdate < date '1995-03-15'
  ...   and l.shipdate > date '1995-03-15'
  ... group by
  ...   l.order_id,
  ...   o.orderdate,
  ...   o.shippriority
  ... order by
  ...   revenue desc,
  ...   o.orderdate
  ... """)


::

  >>> got = (q.lineitem
  ...  .filter(
  ...    (q.order.customer.mktsegment == 'BUILDING') &
  ...    (q.shipdate > date(1995, 3, 15)) &
  ...    (q.order.orderdate < date(1995, 3, 15))
  ...  )
  ...  .group(
  ...    order_id=q.order.id,
  ...    orderdate=q.order.orderdate,
  ...    shippriority=q.order.shippriority,
  ...  )
  ...  .select(
  ...    order_id=q.order_id,
  ...    revenue=q._ >> (q.extendedprice * (1 - q.discount)) >> q.sum(),
  ...    orderdate=q.orderdate,
  ...    shippriority=q.shippriority,
  ...  )
  ...  .sort(q.revenue.desc(), q.orderdate)
  ...  .run())

  >>> got == expected
  True

  >>> got[:3] # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
  [{'order_id': 6240, 'orderdate': '1995-01-28', 'revenue': 245018.0968, 'shippriority': 0},
   {'order_id': 5822, 'orderdate': '1995-03-14', 'revenue': 234486.9328, 'shippriority': 0},
   {'order_id': 9974, 'orderdate': '1995-03-06', 'revenue': 231804.6747, 'shippriority': 0}]

Order Priority Checking Query (Q4)
----------------------------------

SQL::

  >>> expected = execute_sql("""
  ... select
  ...   o.orderpriority,
  ...   count(*) as order_count
  ... from
  ...   "order" o
  ... where
  ...   o.orderdate >= date '1993-07-01'
  ...   and o.orderdate < date '1993-10-01'
  ...   and exists (
  ...     select
  ...       *
  ...     from
  ...       lineitem l
  ...     where
  ...       l.order_id = o.id
  ...       and l.commitdate < l.receiptdate
  ...   )
  ... group by
  ...   o.orderpriority
  ... order by
  ...   o.orderpriority;
  ... """)

::

  >>> got = (q.order
  ...  .filter(
  ...    (q.orderdate >= date(1993, 7, 1)) &
  ...    (q.orderdate < date(1993, 10, 1)) &
  ...    q.lineitem.filter(q.commitdate < q.receiptdate).exists()
  ...  )
  ...  .group(orderpriority=q.orderpriority)
  ...  .select(
  ...    orderpriority=q.orderpriority,
  ...    order_count=q._.count()
  ...  )
  ...  .sort(q.orderpriority)
  ...  .run()) # doctest: +NORMALIZE_WHITESPACE

  >>> got == expected
  True

  >>> got # doctest: +NORMALIZE_WHITESPACE
  [{'order_count': 78, 'orderpriority': '1-URGENT'},
   {'order_count': 80, 'orderpriority': '2-HIGH'},
   {'order_count': 89, 'orderpriority': '3-MEDIUM'},
   {'order_count': 85, 'orderpriority': '4-NOT SPECIFIED'},
   {'order_count': 105, 'orderpriority': '5-LOW'}]

Local Supplier Volume Query (Q5)
--------------------------------

SQL::

  >>> expected = execute_sql("""
  ... select
  ...   n.name as nation,
  ...   sum(l.extendedprice * (1 - l.discount)) as revenue
  ... from
  ...   customer c,
  ...   "order" o,
  ...   lineitem l,
  ...   supplier s,
  ...   partsupp ps,
  ...   nation n,
  ...   region r
  ... where
  ...   c.id = o.customer_id
  ...   and l.order_id = o.id
  ...   and l.partsupp_id = ps.id
  ...   and ps.supplier_id = s.id
  ...   and c.nation_id = s.nation_id
  ...   and s.nation_id = n.id
  ...   and n.region_id = r.id
  ...   and r.name = 'ASIA'
  ...   and o.orderdate >= date '1994-01-01'
  ...   and o.orderdate < date '1995-01-01'
  ... group by
  ...   n.name
  ... order by
  ...   revenue desc
  ... """)

::

  >>> got = (q.lineitem
  ...  .filter(
  ...    (q.partsupp.supplier.nation.region.name == 'ASIA') &
  ...    (q.partsupp.supplier.nation.name == q.order.customer.nation.name) &
  ...    (q.order.orderdate >= date(1994, 1, 1)) &
  ...    (q.order.orderdate < date(1995, 1, 1))
  ...  )
  ...  .group(nation=q.partsupp.supplier.nation.name)
  ...  .select(
  ...    nation=q.nation,
  ...    revenue=q._ >> (q.extendedprice * (1 - q.discount)) >> q.sum(),
  ...  )
  ...  .sort(q.revenue.desc())
  ...  .run()) 

  >>> got == expected
  True

  >>> got # doctest: +NORMALIZE_WHITESPACE
  [{'nation': 'VIETNAM', 'revenue': 807082.6286},
   {'nation': 'INDIA', 'revenue': 697063.011},
   {'nation': 'INDONESIA', 'revenue': 463882.6916},
   {'nation': 'CHINA', 'revenue': 440134.0125},
   {'nation': 'JAPAN', 'revenue': 237479.4272}]

Forecasting Revenue Change Query (Q6)
-------------------------------------

SQL::

  >>> expected = execute_sql("""
  ... select
  ...   sum(l.extendedprice * l.discount) as revenue
  ... from
  ...   lineitem l
  ... where
  ...   l.shipdate >= date '1994-01-01'
  ...   and l.shipdate < date '1995-01-01'
  ...   and l.discount between 0.05 and 0.07
  ...   and l.quantity < 24;
  ... """)

::

  >>> got = ((
  ...  q.lineitem
  ...  .filter(
  ...    (q.shipdate >= date(1994, 1, 1)) &
  ...    (q.shipdate < date(1995, 1, 1)) &
  ...    (q.discount >= 0.05) &
  ...    (q.discount <= 0.07) &
  ...    (q.quantity < 24)
  ...  )
  ...  >> (q.extendedprice * q.discount).sum())
  ...  .run())

  >>> got == expected[0]['revenue']
  True

  >>> got # doctest: +NORMALIZE_WHITESPACE
  905922.8234

Volume Shipping Query (Q7)
--------------------------

SQL::

  >>> expected = execute_sql("""
  ... select
  ...   supp_nation,
  ...   cust_nation,
  ...   l_year as year,
  ...   sum(volume) as revenue
  ... from (
  ...   select
  ...     n1.name as supp_nation,
  ...     n2.name as cust_nation,
  ...     extract(year from l.shipdate) as l_year,
  ...     l.extendedprice * (1 - l.discount) as volume
  ...   from
  ...     supplier s,
  ...     partsupp ps,
  ...     lineitem l,
  ...     "order" o,
  ...     customer c,
  ...     nation n1,
  ...     nation n2
  ...   where
  ...     l.partsupp_id = ps.id
  ...     and s.id = ps.supplier_id
  ...     and o.id = l.order_id
  ...     and c.id = o.customer_id
  ...     and s.nation_id = n1.id
  ...     and c.nation_id = n2.id
  ...     and (
  ...       (n1.name = 'FRANCE' and n2.name = 'GERMANY')
  ...       or (n1.name = 'GERMANY' and n2.name = 'FRANCE')
  ...     )
  ...     and l.shipdate between date '1995-01-01' and date '1996-12-31'
  ... ) as shipping
  ... group by
  ...   supp_nation,
  ...   cust_nation,
  ...   l_year
  ... order by
  ...   supp_nation,
  ...   cust_nation,
  ...   l_year;
  ... """)

::

  >>> got = (q.lineitem
  ...  .filter(
  ...    (
  ...      ((q.order.customer.nation.name == 'GERMANY') &
  ...      (q.partsupp.supplier.nation.name == 'FRANCE')) |
  ...      ((q.order.customer.nation.name == 'FRANCE') &
  ...      (q.partsupp.supplier.nation.name == 'GERMANY'))
  ...    ) &
  ...    (q.shipdate >= date(1995, 1, 1)) &
  ...    (q.shipdate <= date(1996, 12, 31))
  ...  )
  ...  .group(
  ...     year=q.shipdate.year,
  ...     cust_nation=q.order.customer.nation.name,
  ...     supp_nation=q.partsupp.supplier.nation.name,
  ...  )
  ...  .select(
  ...     year=q.year,
  ...     cust_nation=q.cust_nation,
  ...     supp_nation=q.supp_nation,
  ...     revenue=q._ >> (q.extendedprice * (1 - q.discount)) >> q.sum()
  ...  )
  ...  .sort(q.supp_nation, q.cust_nation, q.year)
  ...  .run())

  >>> got == expected
  True

  >>> got # doctest: +NORMALIZE_WHITESPACE
  [{'cust_nation': 'GERMANY', 'revenue': 263047.8824, 'supp_nation': 'FRANCE', 'year': 1995},
   {'cust_nation': 'GERMANY', 'revenue': 154119.1338, 'supp_nation': 'FRANCE', 'year': 1996},
   {'cust_nation': 'FRANCE', 'revenue': 205237.6695, 'supp_nation': 'GERMANY', 'year': 1995},
   {'cust_nation': 'FRANCE', 'revenue': 407967.2149, 'supp_nation': 'GERMANY', 'year': 1996}]

National Market Share Query (Q8)
-------------------------------

SQL::

  >>> expected = execute_sql("""
  ... select
  ...   o_year as year,
  ...   sum(case when nation = 'CANADA' then volume else 0 end) / sum(volume) as mkt_share
  ... from (
  ...   select
  ...     extract(year from o.orderdate) as o_year,
  ...     l.extendedprice * (1 - l.discount) as volume,
  ...     n2.name as nation
  ...   from
  ...     part p,
  ...     partsupp ps,
  ...     supplier s,
  ...     lineitem l,
  ...     "order" o,
  ...     customer c,
  ...     nation n1,
  ...     nation n2,
  ...     region r
  ...   where
  ...     l.partsupp_id = ps.id
  ...     and p.id = ps.part_id
  ...     and s.id = ps.supplier_id
  ...     and l.order_id = o.id
  ...     and o.customer_id = c.id
  ...     and c.nation_id = n1.id
  ...     and n1.region_id = r.id
  ...     and r.name = 'AMERICA'
  ...     and s.nation_id = n2.id
  ...     and o.orderdate between date '1995-01-01' and date '1996-12-31'
  ...     and p.type = 'ECONOMY ANODIZED STEEL'
  ... ) as all_nations
  ... group by
  ...   o_year
  ... order by
  ...   o_year
  ... """)

  >>> q_volume = q.extendedprice * (1 - q.discount)

  >>> got = (
  ... q.lineitem
  ... .filter((q.partsupp.part.type == 'ECONOMY ANODIZED STEEL') &
  ...         (q.order.customer.nation.region.name == 'AMERICA') &
  ...         (q.order.orderdate >= date(1995, 1, 1)) &
  ...         (q.order.orderdate <= date(1996, 12, 31)))
  ... .group(year=q.order.orderdate.year)
  ... .select(
  ...   year=q.year,
  ...   mkt_share=
  ...    (q._.filter(q.partsupp.supplier.nation.name == 'CANADA') >> q_volume).sum() /
  ...    (q._ >> q_volume).sum()
  ... )
  ... .run())

  >>> got == expected
  True

  >>> got # doctest: +NORMALIZE_WHITESPACE
  [{'mkt_share': 0.13794041018126516, 'year': 1995},
   {'mkt_share': 0.26156725927459884, 'year': 1996}]

Product Type Profit Measure Query (Q9)
--------------------------------------

::

  >>> expected = execute_sql("""
  ... select
  ...   nation,
  ...   o_year as year,
  ...   sum(amount) as sum_profit
  ... from (
  ...   select
  ...     n.name as nation,
  ...     extract(year from o.orderdate) as o_year,
  ...     l.extendedprice * (1 - l.discount) - ps.supplycost * l.quantity as amount
  ...   from
  ...     part p,
  ...     supplier s,
  ...     lineitem l,
  ...     partsupp ps,
  ...     "order" o,
  ...     nation n
  ...   where
  ...     l.partsupp_id = ps.id
  ...     and ps.supplier_id = s.id
  ...     and ps.part_id = p.id
  ...     and o.id = l.order_id
  ...     and s.nation_id = n.id
  ...     and p.name ilike '%%green%%'
  ... ) as profit
  ... group by
  ...   nation,
  ...   o_year
  ... order by
  ...   nation,
  ...   o_year desc
  ... """)

::

  >>> q_amount = (
  ...   q.extendedprice * (1 - q.discount) -
  ...   q.partsupp.supplycost * q.quantity
  ... )

::

  >>> got = (
  ...   q.lineitem
  ...   .filter(q.partsupp.part.name.ilike('%green%'))
  ...   .group(
  ...     nation=q.partsupp.supplier.nation.name,
  ...     year=q.order.orderdate.year,
  ...   )
  ...   .sort(q.nation, q.year.desc())
  ...   .select(
  ...     nation=q.nation,
  ...     year=q.year,
  ...     sum_profit=q._ >> q_amount >> q.sum()
  ...   )
  ... ).run()

  >>> got == expected
  True

  >>> got[:10] # doctest: +NORMALIZE_WHITESPACE
  [{'nation': 'ALGERIA', 'sum_profit': 197990.0662, 'year': 1998},
   {'nation': 'ALGERIA', 'sum_profit': 209363.9688, 'year': 1997},
   {'nation': 'ALGERIA', 'sum_profit': 508610.1009, 'year': 1996},
   {'nation': 'ALGERIA', 'sum_profit': 321224.3841, 'year': 1995},
   {'nation': 'ALGERIA', 'sum_profit': 323614.0984, 'year': 1994},
   {'nation': 'ALGERIA', 'sum_profit': 429217.3353, 'year': 1993},
   {'nation': 'ALGERIA', 'sum_profit': 313931.4222, 'year': 1992},
   {'nation': 'ARGENTINA', 'sum_profit': 207703.7187, 'year': 1998},
   {'nation': 'ARGENTINA', 'sum_profit': 404879.3621, 'year': 1997},
   {'nation': 'ARGENTINA', 'sum_profit': 277287.3144, 'year': 1996}]

Returned Item Reporting Query (Q10)
-----------------------------------

::

  >>> expected = execute_sql("""
  ... select
  ...   c.id,
  ...   c.name,
  ...   sum(l.extendedprice * (1 - l.discount)) as revenue,
  ...   c.acctbal,
  ...   n.name as nation,
  ...   c.address,
  ...   c.phone,
  ...   c.comment
  ... from
  ...   customer c,
  ...   "order" o,
  ...   lineitem l,
  ...   nation n
  ... where
  ...   c.id = o.customer_id
  ...   and l.order_id = o.id
  ...   and o.orderdate >= date '1993-10-01'
  ...   and o.orderdate < date '1994-01-01'
  ...   and l.returnflag = 'R'
  ...   and c.nation_id = n.id
  ... group by
  ...   c.id,
  ...   c.name,
  ...   c.acctbal,
  ...   c.phone,
  ...   n.name,
  ...   c.address,
  ...   c.comment
  ... order by
  ...   revenue desc
  ... limit 20
  ... """)

  >>> q_returned = (
  ...   q.order
  ...   .filter((q.orderdate >= date(1993, 10, 1)) &
  ...           (q.orderdate < date(1994, 1, 1)))
  ...   .lineitem
  ...   .filter(q.returnflag == "R")
  ... )

  >>> got = (
  ...   q.customer
  ...   .select(
  ...     id=q.id,
  ...     name=q.name,
  ...     revenue=q_returned >> (q.extendedprice * (1 - q.discount)) >> q.sum(),
  ...     acctbal=q.acctbal,
  ...     nation=q.nation.name,
  ...     address=q.address,
  ...     phone=q.phone,
  ...     comment=q.comment,
  ...   )
  ...   .sort(q.revenue.desc())
  ...   .take(20)
  ... ).run()

  >>> got == expected
  True

  >>> got[:5] # doctest: +NORMALIZE_WHITESPACE
  [{..., 'name': 'Customer#000000544', 'nation': 'ETHIOPIA', ..., 'revenue': 391580.0723},
   {..., 'name': 'Customer#000001105', 'nation': 'RUSSIA', ..., 'revenue': 375872.2968},
   {..., 'name': 'Customer#000000961', 'nation': 'JAPAN', ..., 'revenue': 372764.6176},
   {..., 'name': 'Customer#000000266', 'nation': 'ALGERIA', ..., 'revenue': 347106.7501},
   {..., 'name': 'Customer#000000683', 'nation': 'FRANCE', ..., 'revenue': 328973.7152}]

Important Stock Identification Query (Q11)
------------------------------------------

::

  >>> expected = execute_sql("""
  ... select
  ...   p.name as part,
  ...   sum(ps.supplycost * ps.availqty) as value
  ... from
  ...   partsupp ps,
  ...   supplier s,
  ...   nation n,
  ...   part p
  ... where
  ...   ps.supplier_id = s.id
  ...   and s.nation_id = n.id
  ...   and n.name = 'GERMANY'
  ...   and p.id = ps.part_id
  ... group by
  ...   p.name
  ... having
  ...   sum(ps.supplycost * ps.availqty) > (
  ...     select
  ...       sum(ps.supplycost * ps.availqty) * 0.0001
  ...     from
  ...       partsupp ps,
  ...       supplier s,
  ...       nation n
  ...     where
  ...       ps.supplier_id = s.id
  ...       and s.nation_id = n.id
  ...       and n.name = 'GERMANY'
  ...   )
  ... order by
  ...   value desc;
  ... """)

::

  >>> q_value = (q.supplycost * q.availqty) >> q.sum()

  >>> got = (
  ...   q.partsupp
  ...   .filter(q.supplier.nation.name == 'GERMANY')
  ...   .group(part=q.part.name)
  ...   .filter((q._ >> q_value) > (q.around()._ >> q_value) * 0.0001)
  ...   .select(part=q.part,  value=q._ >> q_value)
  ...   .sort(q.value.desc())
  ... ).run()

  >>> got == expected
  True

  >>> got[:5] # doctest: +NORMALIZE_WHITESPACE
  [{'part': 'almond khaki chartreuse hot seashell', 'value': 13092535.78},
   {'part': 'tan burlywood light chartreuse powder', 'value': 11542206.53},
   {'part': 'grey floral sienna cyan gainsboro', 'value': 9945808.42},
   {'part': 'wheat tomato cyan lemon maroon', 'value': 9941036.4},
   {'part': 'orange cornflower mint snow peach', 'value': 9382317.55}]

Shipping Modes and Order Priority Query (Q12)
---------------------------------------------

::

  >>> expected = execute_sql("""
  ... select
  ...   l.shipmode,
  ...   sum(case
  ...         when o.orderpriority ='1-URGENT' or o.orderpriority = '2-HIGH'
  ...         then 1
  ...         else 0
  ...       end) as high_line_count,
  ...   sum(case
  ...         when o.orderpriority <> '1-URGENT' and o.orderpriority <> '2-HIGH'
  ...         then 1
  ...         else 0
  ...       end) as low_line_count
  ... from
  ...   "order" o,
  ...   lineitem l
  ... where
  ...   o.id = l.order_id
  ...   and l.shipmode in ('MAIL', 'SHIP')
  ...   and l.commitdate < l.receiptdate
  ...   and l.shipdate < l.commitdate
  ...   and l.receiptdate >= date '1994-01-01'
  ...   and l.receiptdate < date '1995-01-01'
  ... group by
  ...   l.shipmode
  ... order by
  ...   l.shipmode
  ... """)

::

  >>> q_high = (
  ...   (q.order.orderpriority == '1-URGENT') |
  ...   (q.order.orderpriority == '2-HIGH')
  ... )

  >>> got = (
  ...   q.lineitem
  ...   .filter((q.shipmode == 'MAIL') | (q.shipmode == 'SHIP'))
  ...   .filter((q.shipdate < q.commitdate) & (q.commitdate < q.receiptdate))
  ...   .filter((q.receiptdate >= date(1994, 1, 1)) & (q.receiptdate < date(1995, 1, 1)))
  ...   .group(shipmode=q.shipmode)
  ...   .select(
  ...      shipmode=q.shipmode,
  ...      high_line_count=q._ >> q.filter(q_high) >> q.count(),
  ...      low_line_count=q._ >> q.filter(~q_high) >> q.count(),
  ...   )
  ...   .sort(q.shipmode)
  ... ).run()

  >>> got == expected
  True

  >>> got # doctest: +NORMALIZE_WHITESPACE
  [{'high_line_count': 45, 'low_line_count': 67, 'shipmode': 'MAIL'},
   {'high_line_count': 45, 'low_line_count': 69, 'shipmode': 'SHIP'}]


Customer Distribution Query (Q13)
---------------------------------

::

  >>> expected = execute_sql("""
  ... select
  ...   count,
  ...   count(*) as custdist
  ... from (
  ...   select
  ...     c.name,
  ...     count(o.key)
  ...   from
  ...     customer c
  ...   left outer join "order" o
  ...     on c.id = o.customer_id and o.comment not like '%%special%%requests%%'
  ...   group by
  ...     c.name) as c_orders (name, count)
  ... group by
  ...   count
  ... order by
  ...   custdist desc,
  ...   count desc
  ... """)

::

  >>> got = (
  ...   q.customer
  ...   .group(count=q.order.filter(~q.comment.like('%special%requests%')).count())
  ...   .select(count=q.count, custdist=q._.count())
  ...   .sort(q.custdist.desc(), q.count.desc())
  ... ).run()

::

  >>> got == expected
  True

  >>> got[:5] # doctest: +NORMALIZE_WHITESPACE
  [{'count': 0, 'custdist': 400},
   {'count': 11, 'custdist': 57},
   {'count': 12, 'custdist': 55},
   {'count': 10, 'custdist': 53},
   {'count': 8, 'custdist': 51}]
