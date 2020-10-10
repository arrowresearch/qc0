from datetime import date
from textwrap import dedent
from sqlalchemy import create_engine, MetaData
from qc0 import q, literal, bind, compile

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


def test_compose_nation_name():
    assert run(q.nation >> q.name) == n(
        """
        SELECT nation_1.name AS value
        FROM nation AS nation_1
        """
    )


def test_compose_nation_region_name():
    assert run(q.nation >> q.region >> q.name) == n(
        """
        SELECT region_1.name AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )


def test_compose_nation_select():
    assert run(
        q.nation >> q.select(nation_name=q.name, region_name=q.region.name),
        print_op=True
    ) == n(
        """
        SELECT jsonb_build_object('nation_name', nation_1.name, 'region_name', region_1.name) AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
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


def test_select_nav_select_nav_multi():
    assert run(
        q.nation.select(
            name=q.name,
            region_name=q.region.name,
            region_comment=q.region.comment,
        ),
        print_op=True,
    ) == n(
        """
        SELECT jsonb_build_object('name', nation_1.name, 'region_name', region_1.name, 'region_comment', region_1.comment) AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )


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


def test_select_nav_select_nav_column():
    assert run(q.region.select(region_name=q.name).region_name) == n(
        """
        SELECT region_1.name AS value
        FROM region AS region_1
        """
    )


def test_select_nav_select_nav_table():
    assert run(q.region.select(n=q.nation).n.name) == n(
        """
        SELECT nation_1.name AS value
        FROM region AS region_1 JOIN nation AS nation_1 ON region_1.id = nation_1.region_id
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


def test_count_region_end():
    assert run(q.region.count()) == n(
        """
        SELECT anon_1.value AS value
        FROM (SELECT count(*) AS value
        FROM region AS region_1) AS anon_1
        """
    )


def test_count_nation_region_end():
    assert run(q.nation.region.count()) == n(
        """
        SELECT anon_1.value AS value
        FROM (SELECT count(*) AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id) AS anon_1
        """
    )


def test_count_region_select_nation_count_end():
    assert run(q.region.select(nation_count=q.nation.count())) == n(
        """
        SELECT jsonb_build_object('nation_count', anon_1.value) AS value
        FROM region AS region_1 LEFT OUTER JOIN LATERAL (SELECT count(*) AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM nation AS nation_1
        WHERE nation_1.region_id = region_1.id) AS anon_2) AS anon_1 ON true
        """
    )


def test_take_region():
    assert run(q.region.take(2)) == n(
        """
        SELECT anon_1.id, anon_1.name, anon_1.comment
        FROM (SELECT region_1.id AS id, region_1.name AS name, region_1.comment AS comment
        FROM region AS region_1
        LIMIT 2) AS anon_1
        """
    )


def test_take_region_nation():
    assert run(q.region.nation.take(2)) == n(
        """
        SELECT anon_1.id, anon_1.name, anon_1.region_id, anon_1.comment
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM region AS region_1 JOIN nation AS nation_1 ON region_1.id = nation_1.region_id
        LIMIT 2) AS anon_1
        """
    )


def test_take_region_x_nation():
    assert run(q.region.take(2).nation) == n(
        """
        SELECT anon_1.id, anon_1.name, anon_1.comment, nation_1.id, nation_1.name, nation_1.region_id, nation_1.comment
        FROM (SELECT region_1.id AS id, region_1.name AS name, region_1.comment AS comment
        FROM region AS region_1
        LIMIT 2) AS anon_1 JOIN nation AS nation_1 ON anon_1.id = nation_1.region_id
        """
    )


def test_filter_region_name():
    assert run(q.region.filter(q.name), print_op=True) == n(
        """
        SELECT anon_1.id, anon_1.name, anon_1.comment
        FROM (SELECT region_1.id AS id, region_1.name AS name, region_1.comment AS comment
        FROM region AS region_1
        WHERE region_1.name) AS anon_1
        """
    )


def test_literal_string():
    assert run(literal("Hello"), print_op=True) == n(
        """
        SELECT 'Hello' AS value
        """
    )


def test_literal_integer():
    assert run(literal(42), print_op=True) == n(
        """
        SELECT 42 AS value
        """
    )


def test_literal_boolean():
    assert run(literal(True), print_op=True) == n(
        """
        SELECT true AS value
        """
    )


def test_filter_region_true():
    assert run(q.region.filter(literal(False)), print_op=True) == n(
        """
        SELECT anon_1.id, anon_1.name, anon_1.comment
        FROM (SELECT region_1.id AS id, region_1.name AS name, region_1.comment AS comment
        FROM region AS region_1
        WHERE false) AS anon_1
        """
    )


def test_filter_region_by_name_end():
    assert run(
        q.region.filter(q.name == literal("AFRICA")), print_op=True
    ) == n(
        """
        SELECT anon_1.id, anon_1.name, anon_1.comment
        FROM (SELECT region_1.id AS id, region_1.name AS name, region_1.comment AS comment
        FROM region AS region_1
        WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )


def test_filter_region_by_name_then_nav_end():
    assert run(
        q.region.filter(q.name == literal("AFRICA")).name, print_op=True
    ) == n(
        """
        SELECT anon_1.name AS value
        FROM (SELECT region_1.id AS id, region_1.name AS name, region_1.comment AS comment
        FROM region AS region_1
        WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )


def test_filter_region_by_name_then_select_end():
    assert run(
        q.region.filter(q.name == literal("AFRICA")).select(
            name=q.name, nation_names=q.nation.name
        ),
        print_op=True,
    ) == n(
        """
        SELECT jsonb_build_object('name', anon_1.name, 'nation_names', anon_2.value) AS value
        FROM (SELECT region_1.id AS id, region_1.name AS name, region_1.comment AS comment
        FROM region AS region_1
        WHERE region_1.name = 'AFRICA') AS anon_1 LEFT OUTER JOIN LATERAL (SELECT jsonb_agg(anon_3.name) AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM nation AS nation_1
        WHERE nation_1.region_id = anon_1.id) AS anon_3) AS anon_2 ON true
        """
    )


def test_filter_nation_by_region_name_end():
    assert run(
        q.nation.filter(q.region.name == literal("AFRICA")),
        print_op=True,
    ) == n(
        """
        SELECT anon_1.id, anon_1.name, anon_1.region_id, anon_1.comment
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )


def test_filter_nation_by_region_name_then_nav_column_end():
    assert run(
        q.nation.filter(q.region.name == literal("AFRICA")).name,
        print_op=True,
    ) == n(
        """
        SELECT anon_1.name AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )


def test_filter_customer_by_region_name_then_nav_column_end():
    assert run(
        q.customer.filter(q.nation.region.name == literal("AFRICA")).name,
        print_op=True,
    ) == n(
        """
        SELECT anon_1.name AS value
        FROM (SELECT customer_1.id AS id, customer_1.name AS name, customer_1.address AS address, customer_1.nation_id AS nation_id, customer_1.phone AS phone, customer_1.acctbal AS acctbal, customer_1.mktsegment AS mktsegment, customer_1.comment AS comment
        FROM customer AS customer_1 JOIN nation AS nation_1 ON customer_1.nation_id = nation_1.id JOIN region AS region_1 ON nation_1.region_id = region_1.id
        WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )


def test_filter_customer_by_region_name_then_count_end():
    assert run(
        q.customer.filter(q.nation.region.name == literal("AFRICA")).count(),
        print_op=True,
    ) == n(
        """
        SELECT anon_1.value AS value
        FROM (SELECT count(*) AS value
        FROM (SELECT customer_1.id AS id, customer_1.name AS name, customer_1.address AS address, customer_1.nation_id AS nation_id, customer_1.phone AS phone, customer_1.acctbal AS acctbal, customer_1.mktsegment AS mktsegment, customer_1.comment AS comment
        FROM customer AS customer_1 JOIN nation AS nation_1 ON customer_1.nation_id = nation_1.id JOIN region AS region_1 ON nation_1.region_id = region_1.id
        WHERE region_1.name = 'AFRICA') AS anon_2) AS anon_1
        """
    )


def test_filter_customer_nation_by_region_name_then_nav_column_end():
    assert run(
        q.customer.nation.filter(q.region.name == literal("AFRICA")).name,
        print_op=True,
    ) == n(
        """
        SELECT anon_1.name AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM customer AS customer_1 JOIN nation AS nation_1 ON customer_1.nation_id = nation_1.id JOIN region AS region_1 ON nation_1.region_id = region_1.id
        WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )


def test_filter_region_by_nation_count():
    assert run(
        q.region.filter(q.nation.count() == literal(5)), print_op=True
    ) == n(
        """
        SELECT anon_1.id, anon_1.name, anon_1.comment
        FROM (SELECT region_1.id AS id, region_1.name AS name, region_1.comment AS comment
        FROM region AS region_1 LEFT OUTER JOIN LATERAL (SELECT count(*) AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM nation AS nation_1
        WHERE nation_1.region_id = region_1.id) AS anon_3) AS anon_2 ON true
        WHERE anon_2.value = 5) AS anon_1
        """
    )


def test_add_string_literals():
    assert run(literal("Hello, ") + literal("World!")) == n(
        """
        SELECT 'Hello, ' || 'World!' AS value
        """
    )


def test_add_integer_literals():
    assert run(literal(40) + literal(2)) == n(
        """
        SELECT 40 + 2 AS value
        """
    )


def test_add_columns():
    assert run(
        q.nation.select(full_name=q.name + literal(" IN ") + q.region.name)
    ) == n(
        """
        SELECT jsonb_build_object('full_name', nation_1.name || ' IN ' || region_1.name) AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )


def test_sub_integer_literals():
    assert run(literal(44) - literal(2)) == n(
        """
        SELECT 44 - 2 AS value
        """
    )


def test_mul_integer_literals():
    assert run(literal(22) * literal(2)) == n(
        """
        SELECT 22 * 2 AS value
        """
    )


def test_truediv_integer_literals():
    assert run(literal(88) / literal(2)) == n(
        """
        SELECT 88 / 2 AS value
        """
    )


def test_and_literals():
    assert run(literal(True) & literal(False)) == n(
        """
        SELECT true AND false AS value
        """
    )


def test_or_literals():
    assert run(literal(True) | literal(False)) == n(
        """
        SELECT true OR false AS value
        """
    )


def test_date_literal():
    assert run(literal(date(2020, 1, 2))) == n(
        """
        SELECT CAST('2020-01-02' AS DATE) AS value
        """
    )


def test_date_literal_nav():
    assert run(literal(date(2020, 1, 2)).year) == n(
        """
        SELECT EXTRACT(year FROM CAST('2020-01-02' AS DATE)) AS value
        """
    )


def test_date_column_nav():
    assert run(q.order.orderdate.year) == n(
        """
        SELECT EXTRACT(year FROM order_1.orderdate) AS value
        FROM "order" AS order_1
        """
    )


def test_json_literal():
    assert run(literal({"hello": ["world"]})) == n(
        """
        SELECT CAST('{"hello": ["world"]}' AS JSONB) AS value
        """
    )


def test_json_literal_nav():
    assert run(literal({"hello": ["world"]}).hello) == n(
        """
        SELECT CAST('{"hello": ["world"]}' AS JSONB) -> 'hello' AS value
        """
    )


def test_json_literal_nested_nav():
    assert run(literal({"hello": {"world": "YES"}}).hello.world) == n(
        """
        SELECT (CAST('{"hello": {"world": "YES"}}' AS JSONB) -> 'hello') -> 'world' AS value
        """
    )
