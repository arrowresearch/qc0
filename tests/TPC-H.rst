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
