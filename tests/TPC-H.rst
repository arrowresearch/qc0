TPC-H
=====

Pricing Summary Report Query (Q1)
---------------------------------

SQL::

  select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
    sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
  from lineitem
  where
    l_shipdate <= date '1998-12-01' - interval '[DELTA]' day
  group by
    l_returnflag,
    l_linestatus
  order by
    l_returnflag,
    l_linestatus;

qc0::

  >>> (q.lineitem
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
  ...  .run()) # doctest: +NORMALIZE_WHITESPACE
  [{'avg_qty': 25.40316106372303,
    'sum_qty': 303771,
    'avg_disc': 0.05011958521491888,
    'avg_price': 33577.13907007861,
    'linestatus': 'F',
    'returnflag': 'A',
    'sum_charge': 396851570.111263,
    'count_order': 11958,
    'sum_base_price': 401515429.0, 
    'sum_disc_price': 381465877.1614},
   {'avg_qty': 25.53472222222222,
    'sum_qty': 7354,
    'avg_disc': 0.04809027777777778,
    'avg_price': 33754.776875,
    'linestatus': 'F',
    'returnflag': 'N',
    'sum_charge': 9616674.437564,
    'count_order': 288,
    'sum_base_price': 9721375.74,
    'sum_disc_price': 9247800.679},
   {'avg_qty': 25.458529473026452,
    'sum_qty': 611132,
    'avg_disc': 0.049931680899812536,
    'avg_price': 33790.326112059985,
    'linestatus': 'O',
    'returnflag': 'N',
    'sum_charge': 801746504.456339,
    'count_order': 24005,
    'sum_base_price': 811136778.32,
    'sum_disc_price': 770866712.5807},
   {'avg_qty': 25.630945415029675,
    'sum_qty': 306623,
    'avg_disc': 0.04973083674663546,
    'avg_price': 33940.52955028003,
    'linestatus': 'F',
    'returnflag': 'R',
    'sum_charge': 401431970.03021,
    'count_order': 11963,
    'sum_base_price': 406030555.01,
    'sum_disc_price': 385792652.5409}]

Minimum Cost Supplier Query (Q2)
--------------------------------

SQL::

  select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
  from
    part,
    supplier,
    partsupp,
    nation,
    region
  where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = [SIZE]
    and p_type like '%[TYPE]'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = '[REGION]'
    and ps_supplycost = (
      select 
        min(ps_supplycost)
      from
        partsupp, supplier,
        nation, region
      where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = '[REGION]'
    )
  order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey;

First, let's query for all ``partsupp`` in the region::

  >>> _ = (q.partsupp
  ...  .filter(q.supplier.nation.region.name == 'EUROPE'))

Now let's keep only those which supply at the minimum cost::

  >>> _ = (q.partsupp
  ...  .filter(q.supplier.nation.region.name == 'EUROPE')
  ...  .filter(q.supplycost == q.fork().supplycost.min()))

Now we can add filters by type and size::

  >>> _ = (q.partsupp
  ...  .filter(q.supplier.nation.region.name == 'EUROPE')
  ...  .filter(q.supplycost == q.fork().supplycost.min())
  ...  .filter(
  ...     q.part.type.like('%NICKEL') &
  ...     (q.part.size == 45)
  ...  ))

Finally we can select needed columns::

  >>> (q.partsupp
  ...  .filter(q.supplier.nation.region.name == 'EUROPE')
  ...  .filter(q.supplycost == q.fork().supplycost.min())
  ...  .filter(
  ...     q.part.type.like('%NICKEL') &
  ...     (q.part.size == 45)
  ...  )
  ...  .select(
  ...     s_acctbal=q.supplier.acctbal,
  ...     s_name=q.supplier.name,
  ...     n_name=q.supplier.nation.name,
  ...     p_mfgr=q.part.mfgr,
  ...     s_address=q.supplier.address,
  ...     s_phone=q.supplier.phone,
  ...     s_comment=q.supplier.comment,
  ...  )
  ...  .run()) # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
  [{'n_name': 'RUSSIA',
    'p_mfgr': 'Manufacturer#1',
    's_name': 'Supplier#000000025',
    's_phone': '32-431-945-3541',
    's_acctbal': 9198.31,
    's_address': 'RCQKONXMFnrodzz6w7fObFVV6CUm2q',
    's_comment': '...'}]

Shipping Priority Query (Q3)
----------------------------

SQL::

  select
    l_orderkey,
    sum(l_extendedprice*(1-l_discount)) as revenue,
    o_orderdate,
    o_shippriority
  from
    customer,
    orders,
    lineitem
  where
    c_mktsegment = '[SEGMENT]'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '[DATE]'
    and l_shipdate > date '[DATE]'
  group by
    l_orderkey,
    o_orderdate,
    o_shippriority
  order by
    revenue desc,
    o_orderdate;

::

  >>> (q.lineitem
  ...  .filter(
  ...    (q.order.customer.mktsegment == 'BUILDING') &
  ...    (q.shipdate > date(1995, 3, 15)) &
  ...    (q.order.orderdate < date(1995, 3, 15))
  ...  )
  ...  .group(
  ...    orderkey=q.order.key,
  ...    orderdate=q.order.orderdate,
  ...    shippriority=q.order.shippriority,
  ...  )
  ...  .select(
  ...    orderkey=q.orderkey,
  ...    revenue=q._ >> (q.extendedprice * (1 - q.discount)) >> q.sum(),
  ...    orderdate=q.orderdate,
  ...    shippriority=q.shippriority,
  ...  )
  ...  .sort(q.revenue.desc(), q.orderdate)
  ...  .take(3)
  ...  .run()) # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
  [{'revenue': 245018.0968, 'orderkey': 24960, 'orderdate': '1995-01-28', 'shippriority': 0},
   {'revenue': 234486.9328, 'orderkey': 23270, 'orderdate': '1995-03-14', 'shippriority': 0},
   {'revenue': 231804.6747, 'orderkey': 39878, 'orderdate': '1995-03-06', 'shippriority': 0}]

Order Priority Checking Query (Q4)
----------------------------------

SQL::

  select
    o_orderpriority,
    count(*) as order_count
  from
    orders
  where
    o_orderdate >= date '[DATE]'
    and o_orderdate < date '[DATE]' + interval '3' month
    and exists (
      select
        *
      from
        lineitem
      where
        l_orderkey = o_orderkey
        and l_commitdate < l_receiptdate
    )
  group by
    o_orderpriority
  order by
    o_orderpriority;

::

  >>> (q.order
  ...  .filter(
  ...    (q.orderdate >= date(1993, 7, 1)) &
  ...    (q.orderdate <= date(1993, 10, 1)) &
  ...    q.lineitem.filter(q.commitdate < q.receiptdate).exists()
  ...  )
  ...  .group(orderpriority=q.orderpriority)
  ...  .select(
  ...    orderpriority=q.orderpriority,
  ...    order_count=q._.count()
  ...  )
  ...  .sort(q.orderpriority)
  ...  .run()) # doctest: +NORMALIZE_WHITESPACE
  [{'order_count': 79, 'orderpriority': '1-URGENT'},
   {'order_count': 80, 'orderpriority': '2-HIGH'},
   {'order_count': 91, 'orderpriority': '3-MEDIUM'},
   {'order_count': 86, 'orderpriority': '4-NOT SPECIFIED'},
   {'order_count': 107, 'orderpriority': '5-LOW'}]

Local Supplier Volume Query (Q5)
--------------------------------

SQL::

  select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
  from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
  where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = '[REGION]'
    and o_orderdate >= date '[DATE]'
    and o_orderdate < date '[DATE]' + interval '1' year
  group by
    n_name
  order by
    revenue desc;

::

  >>> (q.lineitem
  ...  .filter(
  ...    (q.partsupp.supplier.nation.region.name == 'ASIA') &
  ...    (q.order.orderdate >= date(1994, 1, 1)) &
  ...    (q.order.orderdate < date(1995, 1, 1))
  ...  )
  ...  .group(nation=q.partsupp.supplier.nation.name)
  ...  .select(
  ...    nation=q.nation,
  ...    revenue=q._ >> (q.extendedprice * (1 - q.discount)) >> q.sum(),
  ...  )
  ...  .sort(q.revenue.desc())
  ...  .run()) # doctest: +NORMALIZE_WHITESPACE
  [{'nation': 'CHINA', 'revenue': 15111496.4525},
   {'nation': 'INDIA', 'revenue': 15042185.186},
   {'nation': 'INDONESIA', 'revenue': 12900327.9504},
   {'nation': 'VIETNAM', 'revenue': 12673214.0653},
   {'nation': 'JAPAN', 'revenue': 6007285.9402}]

Forecasting Revenue Change Query (Q6)
-------------------------------------

SQL::

  select
    sum(l_extendedprice*l_discount) as revenue
  from
    lineitem
  where
    l_shipdate >= date '[DATE]'
    and l_shipdate < date '[DATE]' + interval '1' year
    and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01
    and l_quantity < [QUANTITY];

::

  >>> ((q.lineitem
  ...  .filter(
  ...    (q.shipdate >= date(1994, 1, 1)) &
  ...    (q.shipdate < date(1995, 1, 1)) &
  ...    (q.discount >= (0.06 - 0.01)) &
  ...    (q.discount <= (0.06 + 0.01)) &
  ...    (q.quantity < 24)
  ...  ) >> (q.extendedprice * (1 - q.discount)).sum())
  ...  .run()) # doctest: +NORMALIZE_WHITESPACE
  Decimal('9548183.0531')
