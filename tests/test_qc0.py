from textwrap import dedent
from sqlalchemy import create_engine, MetaData
from qc0 import q, bind, compile

engine = create_engine("postgresql://")
meta = MetaData()
meta.reflect(bind=engine)


def run(query, print_op=False):
    query = query.syn
    print("-" * 40)
    print(query)

    op = bind(query, meta)
    if print_op:
        print("-" * 40)
        print(op)

    sql = compile(op)
    print("-" * 40)
    sql = sql.compile(engine, compile_kwargs={"literal_binds": True})
    sql = str(sql).strip()
    sql = "\n".join([line.strip() for line in sql.split("\n")])
    print(sql)
    return sql


def n(v):
    return dedent(v).strip()


def test_nav_nation():
    assert run(q.nation) == n(
        """
        SELECT nation_1.id, nation_1.name, nation_1.region_id, nation_1.comment
        FROM nation AS nation_1
        """
    )


def test_nav_nation_name():
    assert run(q.nation.name) == n(
        """
        SELECT nation_1.name AS value
        FROM nation AS nation_1
        """
    )


def test_nav_nation_region_name():
    assert run(q.nation.region.name) == n(
        """
        SELECT region_1.name AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )


def test_nav_customer_nation_region_name():
    assert run(q.customer.nation.region.name, print_op=True) == n(
        """
        SELECT region_1.name AS value
        FROM customer AS customer_1 JOIN nation AS nation_1 ON customer_1.nation_id = nation_1.id JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )


def test_select_nav_select():
    assert run(q.nation.select(name=q.name, comment=q.comment)) == n(
        """
        SELECT jsonb_build_object('name', nation_1.name, 'comment', nation_1.comment) AS value
        FROM nation AS nation_1
        """
    )


def test_select_nav_select_nav_only():
    assert run(q.nation.select(region_name=q.region.name)) == n(
        """
        SELECT jsonb_build_object('region_name', region_1.name) AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )


# def test_select_nav_select_nav_multi():
#     assert run(
#         q.nation.select(
#             name=q.name,
#             region_name=q.region.name,
#             region_comment=q.region.comment,
#         )
#     ) == n(
#         """
#         SELECT jsonb_build_object('name', nation.name, 'region_name', region.name, 'region_comment', region.comment) AS value
#         FROM nation AS nation JOIN region AS region ON region.id = nation.region_id
#         """
#     )


def test_select_nav_select_nav_select():
    assert run(q.nation.select(region=q.region.select(name=q.name))) == n(
        """
        SELECT jsonb_build_object('region', jsonb_build_object('name', region_1.name)) AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )


def test_select_select_nav_one():
    assert run(q.select(region_names=q.region.name), print_op=True) == n(
        """
        SELECT jsonb_build_object('region_names', anon_1.value) AS value
        FROM (SELECT jsonb_agg(region_1.name) AS value
        FROM region AS region_1) AS anon_1
        """
    )


def test_select_select_nav_nav():
    assert run(
        q.select(region_names=q.nation.region.name), print_op=True
    ) == n(
        """
        SELECT jsonb_build_object('region_names', anon_1.value) AS value
        FROM (SELECT jsonb_agg(region_1.name) AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id) AS anon_1
        """
    )


def test_select_select_multiple():
    assert run(
        q.select(nation_names=q.nation.name, region_names=q.region.name),
        print_op=True,
    ) == n(
        """
        SELECT jsonb_build_object('nation_names', anon_1.value, 'region_names', anon_2.value) AS value
        FROM (SELECT jsonb_agg(nation_1.name) AS value
        FROM nation AS nation_1) AS anon_1 JOIN (SELECT jsonb_agg(region_1.name) AS value
        FROM region AS region_1) AS anon_2 ON true
        """
    )


def test_back_nav_region_nation_end():
    assert run(q.region.nation, print_op=True,) == n(
        """
        SELECT region_1.id, region_1.name, region_1.comment, nation_1.id, nation_1.name, nation_1.region_id, nation_1.comment
        FROM region AS region_1 JOIN nation AS nation_1 ON region_1.id = nation_1.region_id
        """
    )


def test_back_nav_region_nation_name_end():
    assert run(q.region.nation.name, print_op=True,) == n(
        """
        SELECT nation_1.name AS value
        FROM region AS region_1 JOIN nation AS nation_1 ON region_1.id = nation_1.region_id
        """
    )


def test_back_nav_region_nation_customer_end():
    assert run(q.region.nation.customer.name, print_op=True,) == n(
        """
        SELECT customer_1.name AS value
        FROM region AS region_1 JOIN nation AS nation_1 ON region_1.id = nation_1.region_id JOIN customer AS customer_1 ON nation_1.id = customer_1.nation_id
        """
    )


def test_select_back_nav():
    assert run(
        q.region.select(nation_names=q.nation.name),
        print_op=True,
    ) == n(
        """
        SELECT jsonb_build_object('nation_names', anon_1.value) AS value
        FROM region AS region_1 LEFT OUTER JOIN LATERAL (SELECT jsonb_agg(anon_2.name) AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM nation AS nation_1
        WHERE nation_1.region_id = region_1.id) AS anon_2) AS anon_1 ON true
        """
    )


def test_select_back_nav_nested_end():
    assert run(
        q.region.select(
            region_name=q.name,
            nations=q.nation.select(
                nation_name=q.name, customer_names=q.customer.name
            ),
        ),
        print_op=True,
    ) == n(
        """
        SELECT jsonb_build_object('region_name', region_1.name, 'nations', anon_1.value) AS value
        FROM region AS region_1 LEFT OUTER JOIN LATERAL (SELECT jsonb_agg(jsonb_build_object('nation_name', anon_2.name, 'customer_names', anon_3.value)) AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM nation AS nation_1
        WHERE nation_1.region_id = region_1.id) AS anon_2 LEFT OUTER JOIN LATERAL (SELECT jsonb_agg(anon_4.name) AS value
        FROM (SELECT customer_1.id AS id, customer_1.name AS name, customer_1.address AS address, customer_1.nation_id AS nation_id, customer_1.phone AS phone, customer_1.acctbal AS acctbal, customer_1.mktsegment AS mktsegment, customer_1.comment AS comment
        FROM customer AS customer_1
        WHERE customer_1.nation_id = anon_2.id) AS anon_4) AS anon_3 ON true) AS anon_1 ON true
        """
    )
