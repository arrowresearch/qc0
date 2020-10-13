import yaml
from datetime import date
from textwrap import dedent
from sqlalchemy import create_engine, MetaData
from qc0 import q, syn_to_op, op_to_sql, execute

engine = create_engine("postgresql://")
meta = MetaData()
meta.reflect(bind=engine)


def run(query, print_op=False):
    query = query.syn
    print("-" * 40)
    print(query)

    op = syn_to_op(query, meta)
    if print_op:
        print("-" * 40)
        print(op)

    sql = op_to_sql(op)
    print("-" * 40)
    sql = sql.compile(engine, compile_kwargs={"literal_binds": True})
    sql = str(sql).strip()
    sql = "\n".join([line.strip() for line in sql.split("\n")])
    print(sql)
    return sql


def n(v):
    return dedent(v).strip()


def assert_result_matches(snapshot, query):
    snapshot.assert_match(yaml.dump(execute(query, meta, engine)))


def test_nav_nation_ok(snapshot):
    query = q.nation
    assert run(query, print_op=True) == n(
        """
        SELECT CAST(row(nation_1.name) AS VARCHAR) AS value
        FROM nation AS nation_1
        """
    )
    assert_result_matches(snapshot, query)


def test_nav_nation_name_ok(snapshot):
    query = q.nation.name
    assert run(query) == n(
        """
        SELECT nation_1.name AS value
        FROM nation AS nation_1
        """
    )
    assert_result_matches(snapshot, query)


def test_nav_nation_region_name_ok(snapshot):
    query = q.nation.region.name
    assert run(query) == n(
        """
        SELECT region_1.name AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )
    assert_result_matches(snapshot, query)


def test_nav_customer_nation_region_name_ok(snapshot):
    query = q.customer.nation.region.name
    assert run(query) == n(
        """
        SELECT region_1.name AS value
        FROM customer AS customer_1 JOIN nation AS nation_1 ON customer_1.nation_id = nation_1.id JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )
    # Too big to test
    # assert_result_matches(snapshot, query)


def test_compose_nation_name_ok(snapshot):
    query = q.nation >> q.name
    assert run(query) == n(
        """
        SELECT nation_1.name AS value
        FROM nation AS nation_1
        """
    )
    assert_result_matches(snapshot, query)


def test_compose_nation_region_name_ok(snapshot):
    query = q.nation >> q.region >> q.name
    assert run(query) == n(
        """
        SELECT region_1.name AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )
    assert_result_matches(snapshot, query)


def test_compose_nation_select_ok(snapshot):
    query = q.nation >> q.select(nation_name=q.name, region_name=q.region.name)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('nation_name', nation_1.name, 'region_name', region_1.name) AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )
    assert_result_matches(snapshot, query)


def test_select_nav_select_ok(snapshot):
    query = q.nation.select(name=q.name, comment=q.comment)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('name', nation_1.name, 'comment', nation_1.comment) AS value
        FROM nation AS nation_1
        """
    )
    assert_result_matches(snapshot, query)


def test_select_nav_select_nav_only_ok(snapshot):
    query = q.nation.select(region_name=q.region.name)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('region_name', region_1.name) AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )
    assert_result_matches(snapshot, query)


def test_select_nav_select_nav_multi_ok(snapshot):
    query = q.nation.select(
        name=q.name,
        region_name=q.region.name,
        region_comment=q.region.comment,
    )
    assert run(query) == n(
        """
        SELECT jsonb_build_object('name', nation_1.name, 'region_name', region_1.name, 'region_comment', region_1.comment) AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )
    assert_result_matches(snapshot, query)


def test_select_nav_select_nav_select_ok(snapshot):
    query = q.nation.select(region=q.region.select(name=q.name))
    assert run(query) == n(
        """
        SELECT jsonb_build_object('region', jsonb_build_object('name', region_1.name)) AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )
    assert_result_matches(snapshot, query)


def test_select_select_nav_one_ok(snapshot):
    query = q.select(region_names=q.region.name)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('region_names', anon_1.value) AS value
        FROM (SELECT jsonb_agg(region_1.name) AS value
        FROM region AS region_1) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_select_select_nav_nav_ok(snapshot):
    query = q.select(region_names=q.nation.region.name)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('region_names', anon_1.value) AS value
        FROM (SELECT jsonb_agg(region_1.name) AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_select_select_multiple_ok(snapshot):
    query = q.select(nation_names=q.nation.name, region_names=q.region.name)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('nation_names', anon_1.value, 'region_names', anon_2.value) AS value
        FROM (SELECT jsonb_agg(nation_1.name) AS value
        FROM nation AS nation_1) AS anon_1 JOIN (SELECT jsonb_agg(region_1.name) AS value
        FROM region AS region_1) AS anon_2 ON true
        """
    )
    assert_result_matches(snapshot, query)


def test_select_nav_select_nav_column_ok(snapshot):
    query = q.region.select(region_name=q.name).region_name
    assert run(query) == n(
        """
        SELECT region_1.name AS value
        FROM region AS region_1
        """
    )
    assert_result_matches(snapshot, query)


def test_select_nav_select_nav_table_ok(snapshot):
    query = q.region.select(n=q.nation).n.name
    assert run(query) == n(
        """
        SELECT nation_1.name AS value
        FROM region AS region_1 JOIN nation AS nation_1 ON region_1.id = nation_1.region_id
        """
    )
    assert_result_matches(snapshot, query)


def test_select_nav_select_select_ok(snapshot):
    query = q.region.select(n=q.nation.name).select(nn=q.n)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('nn', anon_1.value) AS value
        FROM region AS region_1 LEFT OUTER JOIN LATERAL (SELECT jsonb_agg(anon_2.name) AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM nation AS nation_1
        WHERE nation_1.region_id = region_1.id) AS anon_2) AS anon_1 ON true
        """
    )
    assert_result_matches(snapshot, query)


def test_select_nav_select_select_select_ok(snapshot):
    query = q.region.select(n=q.nation.name).select(nn=q.n).select(nnn=q.nn)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('nnn', anon_1.value) AS value
        FROM region AS region_1 LEFT OUTER JOIN LATERAL (SELECT jsonb_agg(anon_2.name) AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM nation AS nation_1
        WHERE nation_1.region_id = region_1.id) AS anon_2) AS anon_1 ON true
        """
    )
    assert_result_matches(snapshot, query)


def test_select_nav_select_select_select_nav_ok(snapshot):
    query = (
        q.region.select(n=q.nation.name).select(nn=q.n).select(nnn=q.nn).nnn
    )
    assert run(query) == n(
        """
        SELECT nation_1.name AS value
        FROM region AS region_1 JOIN nation AS nation_1 ON region_1.id = nation_1.region_id
        """
    )
    assert_result_matches(snapshot, query)


def test_back_nav_region_nation_ok(snapshot):
    query = q.region.nation
    assert run(query) == n(
        """
        SELECT CAST(row(nation_1.name) AS VARCHAR) AS value
        FROM region AS region_1 JOIN nation AS nation_1 ON region_1.id = nation_1.region_id
        """
    )
    assert_result_matches(snapshot, query)


def test_back_nav_region_nation_name_ok(snapshot):
    query = q.region.nation.name
    assert run(query) == n(
        """
        SELECT nation_1.name AS value
        FROM region AS region_1 JOIN nation AS nation_1 ON region_1.id = nation_1.region_id
        """
    )
    assert_result_matches(snapshot, query)


def test_back_nav_region_nation_customer_ok(snapshot):
    query = q.region.nation.customer.name
    assert run(query) == n(
        """
        SELECT customer_1.name AS value
        FROM region AS region_1 JOIN nation AS nation_1 ON region_1.id = nation_1.region_id JOIN customer AS customer_1 ON nation_1.id = customer_1.nation_id
        """
    )
    # Too big to test
    # assert_result_matches(snapshot, query)


def test_select_back_nav_ok(snapshot):
    query = q.region.select(nation_names=q.nation.name)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('nation_names', anon_1.value) AS value
        FROM region AS region_1 LEFT OUTER JOIN LATERAL (SELECT jsonb_agg(anon_2.name) AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM nation AS nation_1
        WHERE nation_1.region_id = region_1.id) AS anon_2) AS anon_1 ON true
        """
    )
    assert_result_matches(snapshot, query)


def test_select_back_nav_nested_ok(snapshot):
    query = q.region.select(
        region_name=q.name,
        nations=q.nation.select(
            nation_name=q.name, customer_names=q.customer.name
        ),
    )
    assert run(query) == n(
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
    # Too big to test
    # assert_result_matches(snapshot, query)


def test_count_region_ok(snapshot):
    query = q.region.count()
    assert run(query) == n(
        """
        SELECT anon_1.value AS value
        FROM (SELECT count(*) AS value
        FROM region AS region_1) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_count_region_via_opend_ok(snapshot):
    query = q.region >> q.count()
    assert run(query) == n(
        """
        SELECT anon_1.value AS value
        FROM (SELECT count(*) AS value
        FROM region AS region_1) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_count_nation_region_ok(snapshot):
    query = q.nation.region.count()
    assert run(query) == n(
        """
        SELECT anon_1.value AS value
        FROM (SELECT count(*) AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_count_region_select_nation_count_ok(snapshot):
    query = q.region.select(nation_count=q.nation.count())
    assert run(query) == n(
        """
        SELECT jsonb_build_object('nation_count', anon_1.value) AS value
        FROM region AS region_1 LEFT OUTER JOIN LATERAL (SELECT count(*) AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM nation AS nation_1
        WHERE nation_1.region_id = region_1.id) AS anon_2) AS anon_1 ON true
        """
    )
    assert_result_matches(snapshot, query)


def test_take_region_ok(snapshot):
    query = q.region.take(2)
    assert run(query) == n(
        """
        SELECT CAST(row(anon_1.name) AS VARCHAR) AS value
        FROM (SELECT region_1.id AS id, region_1.name AS name, region_1.comment AS comment
        FROM region AS region_1
        LIMIT 2) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_take_region_nation_ok(snapshot):
    query = q.region.nation.take(2)
    assert run(query) == n(
        """
        SELECT CAST(row(anon_1.name) AS VARCHAR) AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM region AS region_1 JOIN nation AS nation_1 ON region_1.id = nation_1.region_id
        LIMIT 2) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_take_region_x_nation_ok(snapshot):
    query = q.region.take(2).nation
    assert run(query) == n(
        """
        SELECT CAST(row(nation_1.name) AS VARCHAR) AS value
        FROM (SELECT region_1.id AS id, region_1.name AS name, region_1.comment AS comment
        FROM region AS region_1
        LIMIT 2) AS anon_1 JOIN nation AS nation_1 ON anon_1.id = nation_1.region_id
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_region_name_ok(snapshot):
    query = q.region.filter(q.name == q.val("AFRICA"))
    assert run(query) == n(
        """
        SELECT CAST(row(anon_1.name) AS VARCHAR) AS value
        FROM (SELECT region_1.id AS id, region_1.name AS name, region_1.comment AS comment
        FROM region AS region_1
        WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_literal_string_ok(snapshot):
    query = q.val("Hello")
    assert run(query) == n(
        """
        SELECT 'Hello' AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_literal_integer_ok(snapshot):
    query = q.val(42)
    assert run(query) == n(
        """
        SELECT 42 AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_literal_boolean_ok(snapshot):
    query = q.val(True)
    assert run(query) == n(
        """
        SELECT true AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_literal_composition_with_another_literal_ok(snapshot):
    query = q.val(True) >> q.val(False)
    assert run(query) == n(
        """
        SELECT false AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_literal_composition_with_query_via_op_ok(snapshot):
    query = q.region >> q.val(True)
    assert run(query) == n(
        """
        SELECT true AS value
        FROM region AS region_1
        """
    )
    assert_result_matches(snapshot, query)


def test_literal_composition_with_query_via_dot_ok(snapshot):
    query = q.region.val(True)
    assert run(query) == n(
        """
        SELECT true AS value
        FROM region AS region_1
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_region_true_ok(snapshot):
    query = q.region.filter(q.val(False))
    assert run(query) == n(
        """
        SELECT CAST(row(anon_1.name) AS VARCHAR) AS value
        FROM (SELECT region_1.id AS id, region_1.name AS name, region_1.comment AS comment
        FROM region AS region_1
        WHERE false) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_region_by_name_ok(snapshot):
    query = q.region.filter(q.name == q.val("AFRICA"))
    assert run(query) == n(
        """
        SELECT CAST(row(anon_1.name) AS VARCHAR) AS value
        FROM (SELECT region_1.id AS id, region_1.name AS name, region_1.comment AS comment
        FROM region AS region_1
        WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_region_by_name_then_nav_ok(snapshot):
    query = q.region.filter(q.name == q.val("AFRICA")).name
    assert run(query) == n(
        """
        SELECT anon_1.name AS value
        FROM (SELECT region_1.id AS id, region_1.name AS name, region_1.comment AS comment
        FROM region AS region_1
        WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_region_by_name_then_select_ok(snapshot):
    query = q.region.filter(q.name == q.val("AFRICA")).select(
        name=q.name, nation_names=q.nation.name
    )
    assert run(query) == n(
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
    assert_result_matches(snapshot, query)


def test_filter_nation_by_region_name_ok(snapshot):
    query = q.nation.filter(q.region.name == q.val("AFRICA"))
    assert run(query) == n(
        """
        SELECT CAST(row(anon_1.name) AS VARCHAR) AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_nation_by_region_name_then_nav_column_ok(snapshot):
    query = q.nation.filter(q.region.name == q.val("AFRICA")).name
    assert run(query) == n(
        """
        SELECT anon_1.name AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_customer_by_region_name_then_nav_column_ok(snapshot):
    query = q.customer.filter(q.nation.region.name == q.val("AFRICA")).name
    assert run(query) == n(
        """
        SELECT anon_1.name AS value
        FROM (SELECT customer_1.id AS id, customer_1.name AS name, customer_1.address AS address, customer_1.nation_id AS nation_id, customer_1.phone AS phone, customer_1.acctbal AS acctbal, customer_1.mktsegment AS mktsegment, customer_1.comment AS comment
        FROM customer AS customer_1 JOIN nation AS nation_1 ON customer_1.nation_id = nation_1.id JOIN region AS region_1 ON nation_1.region_id = region_1.id
        WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )
    # Too big to test
    # assert_result_matches(snapshot, query)


def test_filter_customer_by_region_name_then_count_ok(snapshot):
    query = q.customer.filter(q.nation.region.name == q.val("AFRICA")).count()
    assert run(query) == n(
        """
        SELECT anon_1.value AS value
        FROM (SELECT count(*) AS value
        FROM (SELECT customer_1.id AS id, customer_1.name AS name, customer_1.address AS address, customer_1.nation_id AS nation_id, customer_1.phone AS phone, customer_1.acctbal AS acctbal, customer_1.mktsegment AS mktsegment, customer_1.comment AS comment
        FROM customer AS customer_1 JOIN nation AS nation_1 ON customer_1.nation_id = nation_1.id JOIN region AS region_1 ON nation_1.region_id = region_1.id
        WHERE region_1.name = 'AFRICA') AS anon_2) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_customer_nation_by_region_name_then_nav_column_ok(snapshot):
    query = q.customer.nation.filter(q.region.name == q.val("AFRICA")).name
    assert run(query) == n(
        """
        SELECT anon_1.name AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM customer AS customer_1 JOIN nation AS nation_1 ON customer_1.nation_id = nation_1.id JOIN region AS region_1 ON nation_1.region_id = region_1.id
        WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )
    # Too big to test
    # assert_result_matches(snapshot, query)


def test_filter_region_by_nation_count_ok(snapshot):
    query = q.region.filter(q.nation.count() == q.val(5))
    assert run(query) == n(
        """
        SELECT CAST(row(anon_1.name) AS VARCHAR) AS value
        FROM (SELECT region_1.id AS id, region_1.name AS name, region_1.comment AS comment
        FROM region AS region_1 LEFT OUTER JOIN LATERAL (SELECT count(*) AS value
        FROM (SELECT nation_1.id AS id, nation_1.name AS name, nation_1.region_id AS region_id, nation_1.comment AS comment
        FROM nation AS nation_1
        WHERE nation_1.region_id = region_1.id) AS anon_3) AS anon_2 ON true
        WHERE anon_2.value = 5) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_add_string_literals_ok(snapshot):
    query = q.val("Hello, ") + q.val("World!")
    assert run(query) == n(
        """
        SELECT 'Hello, ' || 'World!' AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_add_integer_literals_ok(snapshot):
    query = q.val(40) + q.val(2)
    assert run(query) == n(
        """
        SELECT 40 + 2 AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_add_columns_ok(snapshot):
    query = q.nation.select(full_name=q.name + q.val(" IN ") + q.region.name)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('full_name', nation_1.name || ' IN ' || region_1.name) AS value
        FROM nation AS nation_1 JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )
    assert_result_matches(snapshot, query)


def test_sub_integer_literals_ok(snapshot):
    query = q.val(44) - q.val(2)
    assert run(query) == n(
        """
        SELECT 44 - 2 AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_mul_integer_literals_ok(snapshot):
    query = q.val(22) * q.val(2)
    assert run(query) == n(
        """
        SELECT 22 * 2 AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_truediv_integer_literals_ok(snapshot):
    query = q.val(88) / q.val(2)
    assert run(query) == n(
        """
        SELECT 88 / 2 AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_and_literals_ok(snapshot):
    query = q.val(True) & q.val(False)
    assert run(query) == n(
        """
        SELECT true AND false AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_or_literals_ok(snapshot):
    query = q.val(True) | q.val(False)
    assert run(query) == n(
        """
        SELECT true OR false AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_date_literal_ok(snapshot):
    query = q.val(date(2020, 1, 2))
    assert run(query) == n(
        """
        SELECT CAST('2020-01-02' AS DATE) AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_date_literal_nav_ok(snapshot):
    query = q.val(date(2020, 1, 2)).year
    assert run(query) == n(
        """
        SELECT EXTRACT(year FROM CAST('2020-01-02' AS DATE)) AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_date_column_nav_ok(snapshot):
    query = q.order.orderdate.year
    assert run(query) == n(
        """
        SELECT EXTRACT(year FROM order_1.orderdate) AS value
        FROM "order" AS order_1
        """
    )
    # Too big to test
    # assert_result_matches(snapshot, query)


def test_json_literal_ok(snapshot):
    query = q.val({"hello": ["world"]})
    assert run(query) == n(
        """
        SELECT CAST('{"hello": ["world"]}' AS JSONB) AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_json_literal_nav_ok(snapshot):
    query = q.val({"hello": ["world"]}).hello
    assert run(query) == n(
        """
        SELECT CAST('{"hello": ["world"]}' AS JSONB) -> 'hello' AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_json_literal_nested_nav_ok(snapshot):
    query = q.val({"hello": {"world": "YES"}}).hello.world
    assert run(query) == n(
        """
        SELECT (CAST('{"hello": {"world": "YES"}}' AS JSONB) -> 'hello') -> 'world' AS value
        """
    )
    assert_result_matches(snapshot, query)
