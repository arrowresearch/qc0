import pytest
import yaml
from datetime import date
from textwrap import dedent
from sqlalchemy import create_engine, MetaData
from qc0 import Q

engine = create_engine("postgresql://")
meta = MetaData()
meta.reflect(bind=engine)
q = Q(meta=meta, engine=engine)


def run(query, print_op=False):
    print("-" * 40)
    print(query.syn)

    if print_op:
        print("-" * 40)
        query.print_op()

    sql = query.sql
    print("-" * 40)
    print(sql)
    return sql


def n(v):
    return dedent(v).strip()


def assert_result_matches(snapshot, query):
    snapshot.assert_match(yaml.dump(query.run()))


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
    assert run(query, print_op=True) == n(
        """
        SELECT nation_1.name AS value
        FROM nation AS nation_1
        """
    )
    assert_result_matches(snapshot, query)


def test_nav_nation_region_name_ok(snapshot):
    query = q.nation.region.name
    assert run(query, print_op=True) == n(
        """
        SELECT region_1.name AS value
        FROM nation AS nation_1
        JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )
    assert_result_matches(snapshot, query)


def test_nav_customer_nation_region_name_ok(snapshot):
    query = q.customer.nation.region.name
    assert run(query) == n(
        """
        SELECT region_1.name AS value
        FROM customer AS customer_1
        JOIN nation AS nation_1 ON customer_1.nation_id = nation_1.id
        JOIN region AS region_1 ON nation_1.region_id = region_1.id
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
        FROM nation AS nation_1
        JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )
    assert_result_matches(snapshot, query)


def test_compose_nation_select_ok(snapshot):
    query = q.nation >> q.select(nation_name=q.name, region_name=q.region.name)
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('nation_name', nation_1.name, 'region_name', region_1.name) AS value
        FROM nation AS nation_1
        JOIN region AS region_1 ON nation_1.region_id = region_1.id
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


def test_select_tables_ok(snapshot):
    query = q.select(region=q.region)
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('region', anon_1.value) AS value
        FROM
          (SELECT jsonb_agg(CAST(row(region_1.name) AS VARCHAR)) AS value
           FROM region AS region_1) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_select_nav_select_nav_only_ok(snapshot):
    query = q.nation.select(region_name=q.region.name)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('region_name', region_1.name) AS value
        FROM nation AS nation_1
        JOIN region AS region_1 ON nation_1.region_id = region_1.id
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
        FROM nation AS nation_1
        JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )
    assert_result_matches(snapshot, query)


def test_select_nav_select_nav_select_ok(snapshot):
    query = q.nation.select(region=q.region.select(name=q.name))
    assert run(query) == n(
        """
        SELECT jsonb_build_object('region', jsonb_build_object('name', region_1.name)) AS value
        FROM nation AS nation_1
        JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )
    assert_result_matches(snapshot, query)


def test_select_select_nav_one_ok(snapshot):
    query = q.select(region_names=q.region.name)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('region_names', anon_1.value) AS value
        FROM
          (SELECT jsonb_agg(region_1.name) AS value
           FROM region AS region_1) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_select_select_nav_nav_ok(snapshot):
    query = q.select(region_names=q.nation.region.name)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('region_names', anon_1.value) AS value
        FROM
          (SELECT jsonb_agg(region_1.name) AS value
           FROM nation AS nation_1
           JOIN region AS region_1 ON nation_1.region_id = region_1.id) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_select_select_multiple_ok(snapshot):
    query = q.select(nation_names=q.nation.name, region_names=q.region.name)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('nation_names', anon_1.value, 'region_names', anon_2.value) AS value
        FROM
          (SELECT jsonb_agg(nation_1.name) AS value
           FROM nation AS nation_1) AS anon_1
        JOIN
          (SELECT jsonb_agg(region_1.name) AS value
           FROM region AS region_1) AS anon_2 ON TRUE
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
    assert run(query, print_op=True) == n(
        """
        SELECT nation_1.name AS value
        FROM region AS region_1
        JOIN nation AS nation_1 ON region_1.id = nation_1.region_id
        """
    )
    assert_result_matches(snapshot, query)


def test_select_nav_select_select_ok(snapshot):
    query = q.region.select(n=q.nation.name).select(nn=q.n)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('nn', anon_1.value) AS value
        FROM region AS region_1
        LEFT OUTER JOIN LATERAL
          (SELECT jsonb_agg(anon_2.name) AS value
           FROM
             (SELECT nation_1.id AS id,
                     nation_1.name AS name,
                     nation_1.region_id AS region_id,
                     nation_1.comment AS COMMENT
              FROM nation AS nation_1
              WHERE nation_1.region_id = region_1.id) AS anon_2) AS anon_1 ON TRUE
        """
    )
    assert_result_matches(snapshot, query)


def test_select_nav_select_select_select_ok(snapshot):
    query = q.region.select(n=q.nation.name).select(nn=q.n).select(nnn=q.nn)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('nnn', anon_1.value) AS value
        FROM region AS region_1
        LEFT OUTER JOIN LATERAL
          (SELECT jsonb_agg(anon_2.name) AS value
           FROM
             (SELECT nation_1.id AS id,
                     nation_1.name AS name,
                     nation_1.region_id AS region_id,
                     nation_1.comment AS COMMENT
              FROM nation AS nation_1
              WHERE nation_1.region_id = region_1.id) AS anon_2) AS anon_1 ON TRUE
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
        FROM region AS region_1
        JOIN nation AS nation_1 ON region_1.id = nation_1.region_id
        """
    )
    assert_result_matches(snapshot, query)


def test_back_nav_region_nation_ok(snapshot):
    query = q.region.nation
    assert run(query) == n(
        """
        SELECT CAST(row(nation_1.name) AS VARCHAR) AS value
        FROM region AS region_1
        JOIN nation AS nation_1 ON region_1.id = nation_1.region_id
        """
    )
    assert_result_matches(snapshot, query)


def test_back_nav_region_nation_name_ok(snapshot):
    query = q.region.nation.name
    assert run(query) == n(
        """
        SELECT nation_1.name AS value
        FROM region AS region_1
        JOIN nation AS nation_1 ON region_1.id = nation_1.region_id
        """
    )
    assert_result_matches(snapshot, query)


def test_back_nav_region_nation_customer_ok(snapshot):
    query = q.region.nation.customer.name
    assert run(query) == n(
        """
        SELECT customer_1.name AS value
        FROM region AS region_1
        JOIN nation AS nation_1 ON region_1.id = nation_1.region_id
        JOIN customer AS customer_1 ON nation_1.id = customer_1.nation_id
        """
    )
    # Too big to test
    # assert_result_matches(snapshot, query)


def test_select_back_nav_ok(snapshot):
    query = q.region.select(nation_names=q.nation.name)
    assert run(query) == n(
        """
        SELECT jsonb_build_object('nation_names', anon_1.value) AS value
        FROM region AS region_1
        LEFT OUTER JOIN LATERAL
          (SELECT jsonb_agg(anon_2.name) AS value
           FROM
             (SELECT nation_1.id AS id,
                     nation_1.name AS name,
                     nation_1.region_id AS region_id,
                     nation_1.comment AS COMMENT
              FROM nation AS nation_1
              WHERE nation_1.region_id = region_1.id) AS anon_2) AS anon_1 ON TRUE
        """
    )
    assert_result_matches(snapshot, query)


def test_select_back_nav_nested_ok(snapshot):
    query = q.region.select(
        region_name=q.name,
        nations=q.nation.select(
            nation_name=q.name,
            customer_names=q.customer.name,
        ),
    )
    assert run(query) == n(
        """
        SELECT jsonb_build_object('region_name', region_1.name, 'nations', anon_1.value) AS value
        FROM region AS region_1
        LEFT OUTER JOIN LATERAL
          (SELECT jsonb_agg(jsonb_build_object('nation_name', anon_2.name, 'customer_names', anon_3.value)) AS value
           FROM
             (SELECT nation_1.id AS id,
                     nation_1.name AS name,
                     nation_1.region_id AS region_id,
                     nation_1.comment AS COMMENT
              FROM nation AS nation_1
              WHERE nation_1.region_id = region_1.id) AS anon_2
           LEFT OUTER JOIN LATERAL
             (SELECT jsonb_agg(anon_4.name) AS value
              FROM
                (SELECT customer_1.id AS id,
                        customer_1.name AS name,
                        customer_1.address AS address,
                        customer_1.nation_id AS nation_id,
                        customer_1.phone AS phone,
                        customer_1.acctbal AS acctbal,
                        customer_1.mktsegment AS mktsegment,
                        customer_1.comment AS COMMENT
                 FROM customer AS customer_1
                 WHERE customer_1.nation_id = anon_2.id) AS anon_4) AS anon_3 ON TRUE) AS anon_1 ON TRUE
        """
    )
    # Too big to test
    # assert_result_matches(snapshot, query)


def test_count_region_ok(snapshot):
    query = q.region.count()
    assert run(query, print_op=True) == n(
        """
        SELECT anon_1.value AS value
        FROM
          (SELECT count(*) AS value
           FROM region AS region_1) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_count_region_via_opend_ok(snapshot):
    query = q.region >> q.count()
    assert run(query) == n(
        """
        SELECT anon_1.value AS value
        FROM
          (SELECT count(*) AS value
           FROM region AS region_1) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_count_nation_region_ok(snapshot):
    query = q.nation.region.count()
    assert run(query) == n(
        """
        SELECT anon_1.value AS value
        FROM
          (SELECT count(*) AS value
           FROM nation AS nation_1
           JOIN region AS region_1 ON nation_1.region_id = region_1.id) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_count_region_select_nation_count_ok(snapshot):
    query = q.region.select(nation_count=q.nation.count())
    assert run(query) == n(
        """
        SELECT jsonb_build_object('nation_count', anon_1.value) AS value
        FROM region AS region_1
        LEFT OUTER JOIN LATERAL
          (SELECT count(*) AS value
           FROM
             (SELECT nation_1.id AS id,
                     nation_1.name AS name,
                     nation_1.region_id AS region_id,
                     nation_1.comment AS COMMENT
              FROM nation AS nation_1
              WHERE nation_1.region_id = region_1.id) AS anon_2) AS anon_1 ON TRUE
        """
    )
    assert_result_matches(snapshot, query)


def test_take_region_ok(snapshot):
    query = q.region.take(2)
    assert run(query) == n(
        """
        SELECT CAST(row(anon_1.name) AS VARCHAR) AS value
        FROM
          (SELECT region_1.id AS id,
                  region_1.name AS name,
                  region_1.comment AS COMMENT
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
        FROM
          (SELECT nation_1.id AS id,
                  nation_1.name AS name,
                  nation_1.region_id AS region_id,
                  nation_1.comment AS COMMENT
           FROM region AS region_1
           JOIN nation AS nation_1 ON region_1.id = nation_1.region_id
           LIMIT 2) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_take_region_x_nation_ok(snapshot):
    query = q.region.take(2).nation
    assert run(query) == n(
        """
        SELECT CAST(row(nation_1.name) AS VARCHAR) AS value
        FROM
          (SELECT region_1.id AS id,
                  region_1.name AS name,
                  region_1.comment AS COMMENT
           FROM region AS region_1
           LIMIT 2) AS anon_1
        JOIN nation AS nation_1 ON anon_1.id = nation_1.region_id
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_region_name_ok(snapshot):
    query = q.region.filter(q.name == "AFRICA")
    assert run(query, print_op=True) == n(
        """
        SELECT CAST(row(anon_1.name) AS VARCHAR) AS value
        FROM
          (SELECT region_1.id AS id,
                  region_1.name AS name,
                  region_1.comment AS COMMENT
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
        SELECT TRUE AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_literal_composition_with_another_literal_ok(snapshot):
    query = q.val(True) >> q.val(False)
    assert run(query) == n(
        """
        SELECT FALSE AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_literal_composition_with_query_via_op_ok(snapshot):
    query = q.region >> True
    assert run(query, print_op=True) == n(
        """
        SELECT TRUE AS value
        FROM region AS region_1
        """
    )
    assert_result_matches(snapshot, query)


def test_literal_composition_with_query_via_dot_ok(snapshot):
    query = q.region.val(True)
    assert run(query) == n(
        """
        SELECT TRUE AS value
        FROM region AS region_1
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_region_true_ok(snapshot):
    query = q.region.filter(False)
    assert run(query) == n(
        """
        SELECT CAST(row(anon_1.name) AS VARCHAR) AS value
        FROM
          (SELECT region_1.id AS id,
                  region_1.name AS name,
                  region_1.comment AS COMMENT
           FROM region AS region_1
           WHERE FALSE) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_region_by_name_ok(snapshot):
    query = q.region.filter(q.name == "AFRICA")
    assert run(query) == n(
        """
        SELECT CAST(row(anon_1.name) AS VARCHAR) AS value
        FROM
          (SELECT region_1.id AS id,
                  region_1.name AS name,
                  region_1.comment AS COMMENT
           FROM region AS region_1
           WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_region_by_name_then_nav_ok(snapshot):
    query = q.region.filter(q.name == "AFRICA").name
    assert run(query) == n(
        """
        SELECT anon_1.name AS value
        FROM
          (SELECT region_1.id AS id,
                  region_1.name AS name,
                  region_1.comment AS COMMENT
           FROM region AS region_1
           WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_region_by_name_then_select_ok(snapshot):
    query = q.region.filter(q.name == "AFRICA").select(
        name=q.name, nation_names=q.nation.name
    )
    assert run(query) == n(
        """
        SELECT jsonb_build_object('name', anon_1.name, 'nation_names', anon_2.value) AS value
        FROM
          (SELECT region_1.id AS id,
                  region_1.name AS name,
                  region_1.comment AS COMMENT
           FROM region AS region_1
           WHERE region_1.name = 'AFRICA') AS anon_1
        LEFT OUTER JOIN LATERAL
          (SELECT jsonb_agg(anon_3.name) AS value
           FROM
             (SELECT nation_1.id AS id,
                     nation_1.name AS name,
                     nation_1.region_id AS region_id,
                     nation_1.comment AS COMMENT
              FROM nation AS nation_1
              WHERE nation_1.region_id = anon_1.id) AS anon_3) AS anon_2 ON TRUE
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_nation_by_region_name_ok(snapshot):
    query = q.nation.filter(q.region.name == "AFRICA")
    assert run(query) == n(
        """
        SELECT CAST(row(anon_1.name) AS VARCHAR) AS value
        FROM
          (SELECT nation_1.id AS id,
                  nation_1.name AS name,
                  nation_1.region_id AS region_id,
                  nation_1.comment AS COMMENT
           FROM nation AS nation_1
           JOIN region AS region_1 ON nation_1.region_id = region_1.id
           WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_nation_by_region_name_then_nav_column_ok(snapshot):
    query = q.nation.filter(q.region.name == "AFRICA").name
    assert run(query) == n(
        """
        SELECT anon_1.name AS value
        FROM
          (SELECT nation_1.id AS id,
                  nation_1.name AS name,
                  nation_1.region_id AS region_id,
                  nation_1.comment AS COMMENT
           FROM nation AS nation_1
           JOIN region AS region_1 ON nation_1.region_id = region_1.id
           WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_customer_by_region_name_then_nav_column_ok(snapshot):
    query = q.customer.filter(q.nation.region.name == "AFRICA").name
    assert run(query) == n(
        """
        SELECT anon_1.name AS value
        FROM
          (SELECT customer_1.id AS id,
                  customer_1.name AS name,
                  customer_1.address AS address,
                  customer_1.nation_id AS nation_id,
                  customer_1.phone AS phone,
                  customer_1.acctbal AS acctbal,
                  customer_1.mktsegment AS mktsegment,
                  customer_1.comment AS COMMENT
           FROM customer AS customer_1
           JOIN nation AS nation_1 ON customer_1.nation_id = nation_1.id
           JOIN region AS region_1 ON nation_1.region_id = region_1.id
           WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )
    # Too big to test
    # assert_result_matches(snapshot, query)


def test_filter_customer_by_region_name_then_count_ok(snapshot):
    query = q.customer.filter(q.nation.region.name == "AFRICA").count()
    assert run(query) == n(
        """
        SELECT anon_1.value AS value
        FROM
          (SELECT count(*) AS value
           FROM
             (SELECT customer_1.id AS id,
                     customer_1.name AS name,
                     customer_1.address AS address,
                     customer_1.nation_id AS nation_id,
                     customer_1.phone AS phone,
                     customer_1.acctbal AS acctbal,
                     customer_1.mktsegment AS mktsegment,
                     customer_1.comment AS COMMENT
              FROM customer AS customer_1
              JOIN nation AS nation_1 ON customer_1.nation_id = nation_1.id
              JOIN region AS region_1 ON nation_1.region_id = region_1.id
              WHERE region_1.name = 'AFRICA') AS anon_2) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_filter_customer_nation_by_region_name_then_nav_column_ok(snapshot):
    query = q.customer.nation.filter(q.region.name == "AFRICA").name
    assert run(query) == n(
        """
        SELECT anon_1.name AS value
        FROM
          (SELECT nation_1.id AS id,
                  nation_1.name AS name,
                  nation_1.region_id AS region_id,
                  nation_1.comment AS COMMENT
           FROM customer AS customer_1
           JOIN nation AS nation_1 ON customer_1.nation_id = nation_1.id
           JOIN region AS region_1 ON nation_1.region_id = region_1.id
           WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )
    # Too big to test
    # assert_result_matches(snapshot, query)


def test_filter_region_by_nation_count_ok(snapshot):
    query = q.region.filter(q.nation.count() == 5)
    assert run(query) == n(
        """
        SELECT CAST(row(anon_1.name) AS VARCHAR) AS value
        FROM
          (SELECT region_1.id AS id,
                  region_1.name AS name,
                  region_1.comment AS COMMENT
           FROM region AS region_1
           LEFT OUTER JOIN LATERAL
             (SELECT count(*) AS value
              FROM
                (SELECT nation_1.id AS id,
                        nation_1.name AS name,
                        nation_1.region_id AS region_id,
                        nation_1.comment AS COMMENT
                 FROM nation AS nation_1
                 WHERE nation_1.region_id = region_1.id) AS anon_3) AS anon_2 ON TRUE
           WHERE anon_2.value = 5) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_add_string_literals_ok(snapshot):
    query = q.val("Hello, ") + "World!"
    assert run(query) == n(
        """
        SELECT 'Hello, ' || 'World!' AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_add_integer_literals_ok(snapshot):
    query = q.val(40) + 2
    assert run(query, print_op=True) == n(
        """
        SELECT 40 + 2 AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_add_columns_ok(snapshot):
    query = q.nation.select(full_name=q.name + " IN " + q.region.name)
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('full_name', nation_1.name || ' IN ' || region_1.name) AS value
        FROM nation AS nation_1
        JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )
    assert_result_matches(snapshot, query)


def test_add_lateral_columns_ok(snapshot):
    query = q.region.select(names=q.nation.name + "!")
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('names', anon_1.value) AS value
        FROM region AS region_1
        LEFT OUTER JOIN LATERAL
          (SELECT jsonb_agg(anon_2.name || '!') AS value
           FROM
             (SELECT nation_1.id AS id,
                     nation_1.name AS name,
                     nation_1.region_id AS region_id,
                     nation_1.comment AS COMMENT
              FROM nation AS nation_1
              WHERE nation_1.region_id = region_1.id) AS anon_2) AS anon_1 ON TRUE
        """
    )
    assert_result_matches(snapshot, query)


def test_add_columns_of_relok(snapshot):
    query = q.region.name + "!"
    assert run(query, print_op=True) == n(
        """
        SELECT region_1.name || '!' AS value
        FROM region AS region_1
        """
    )
    assert_result_matches(snapshot, query)


def test_sub_integer_literals_ok(snapshot):
    query = q.val(44) - 2
    assert run(query) == n(
        """
        SELECT 44 - 2 AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_mul_integer_literals_ok(snapshot):
    query = q.val(22) * 2
    assert run(query) == n(
        """
        SELECT 22 * 2 AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_truediv_integer_literals_ok(snapshot):
    query = q.val(88) / 2
    assert run(query) == n(
        """
        SELECT 88 / 2 AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_and_literals_ok(snapshot):
    query = q.val(True) & False
    assert run(query) == n(
        """
        SELECT TRUE
        AND FALSE AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_or_literals_ok(snapshot):
    query = q.val(True) | False
    assert run(query) == n(
        """
        SELECT TRUE
        OR FALSE AS value
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
        SELECT EXTRACT(YEAR
                       FROM CAST('2020-01-02' AS DATE)) AS value
        """
    )
    assert_result_matches(snapshot, query)


def test_date_column_nav_ok(snapshot):
    query = q.order.orderdate.year
    assert run(query) == n(
        """
        SELECT EXTRACT(YEAR
                       FROM order_1.orderdate) AS value
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


def test_select_filter_end(snapshot):
    query = q.region.select(n=q.name).filter(q.n == "AFRICA")
    assert run(query) == n(
        """
        SELECT jsonb_build_object('n', anon_1.name) AS value
        FROM
          (SELECT region_1.id AS id,
                  region_1.name AS name,
                  region_1.comment AS COMMENT
           FROM region AS region_1
           WHERE region_1.name = 'AFRICA') AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_group_nation_by_region_select_ok(snapshot):
    query = q.nation.group(reg=q.region.name).select(reg=q.reg)
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('reg', anon_1.reg) AS value
        FROM
          (SELECT region_1.name AS reg
           FROM nation AS nation_1
           JOIN region AS region_1 ON nation_1.region_id = region_1.id
           GROUP BY region_1.name) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_group_region_by_nation_select_ok(snapshot):
    query = q.region.group(reg=q.nation.name.count()).select(field=q.reg)
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('field', anon_1.reg) AS value
        FROM
          (SELECT anon_2.value AS reg
           FROM region AS region_1
           LEFT OUTER JOIN LATERAL
             (SELECT count(anon_3.name) AS value
              FROM
                (SELECT nation_1.id AS id,
                        nation_1.name AS name,
                        nation_1.region_id AS region_id,
                        nation_1.comment AS COMMENT
                 FROM nation AS nation_1
                 WHERE nation_1.region_id = region_1.id) AS anon_3) AS anon_2 ON TRUE
           GROUP BY anon_2.value) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_group_nation_by_region_select_aggr_ok(snapshot):
    query = q.nation.group(reg=q.region.name).select(
        ref=q.reg,
        count=q._.name.count(),
    )
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('ref', anon_1.reg, 'count', anon_1.aggr_0) AS value
        FROM
          (SELECT anon_2.reg AS reg,
                  coalesce(anon_3.value, 0) AS aggr_0
           FROM
             (SELECT region_1.name AS reg
              FROM nation AS nation_1
              JOIN region AS region_1 ON nation_1.region_id = region_1.id
              GROUP BY region_1.name) AS anon_2
           LEFT OUTER JOIN
             (SELECT region_2.name AS reg,
                     count(nation_1.name) AS value
              FROM nation AS nation_1
              JOIN region AS region_2 ON nation_1.region_id = region_2.id
              GROUP BY region_2.name) AS anon_3 ON anon_2.reg = anon_3.reg) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_group_nation_by_region_select_aggr_link_ok(snapshot):
    query = q.nation.group(reg=q.region.name).select(
        ref=q.reg,
        customer_count=q._.customer.count(),
    )
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('ref', anon_1.reg, 'customer_count', anon_1.aggr_0) AS value
        FROM
          (SELECT anon_2.reg AS reg,
                  coalesce(anon_3.value, 0) AS aggr_0
           FROM
             (SELECT region_1.name AS reg
              FROM nation AS nation_1
              JOIN region AS region_1 ON nation_1.region_id = region_1.id
              GROUP BY region_1.name) AS anon_2
           LEFT OUTER JOIN
             (SELECT region_2.name AS reg,
                     count(*) AS value
              FROM nation AS nation_1
              JOIN region AS region_2 ON nation_1.region_id = region_2.id
              JOIN customer AS customer_1 ON nation_1.id = customer_1.nation_id
              GROUP BY region_2.name) AS anon_3 ON anon_2.reg = anon_3.reg) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_group_nation_by_region_select_aggr_col_and_link_ok(snapshot):
    query = q.nation.group(reg=q.region.name).select(
        ref=q.reg,
        nation_count=q._.name.count(),
        customer_count=q._.customer.count(),
    )
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('ref', anon_1.reg, 'nation_count', anon_1.aggr_0, 'customer_count', anon_1.aggr_1) AS value
        FROM
          (SELECT anon_2.reg AS reg,
                  coalesce(anon_3.value, 0) AS aggr_0,
                  coalesce(anon_4.value, 0) AS aggr_1
           FROM
             (SELECT region_1.name AS reg
              FROM nation AS nation_1
              JOIN region AS region_1 ON nation_1.region_id = region_1.id
              GROUP BY region_1.name) AS anon_2
           LEFT OUTER JOIN
             (SELECT region_2.name AS reg,
                     count(nation_1.name) AS value
              FROM nation AS nation_1
              JOIN region AS region_2 ON nation_1.region_id = region_2.id
              GROUP BY region_2.name) AS anon_3 ON anon_2.reg = anon_3.reg
           LEFT OUTER JOIN
             (SELECT region_3.name AS reg,
                     count(*) AS value
              FROM nation AS nation_1
              JOIN region AS region_3 ON nation_1.region_id = region_3.id
              JOIN customer AS customer_1 ON nation_1.id = customer_1.nation_id
              GROUP BY region_3.name) AS anon_4 ON anon_3.reg = anon_4.reg) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_group_nation_by_region_select_aggr_filter_ok(snapshot):
    query = q.nation.group(reg=q.region.name).select(
        reg=q.reg,
        count=q._.filter(q.name == "KENYA").count(),
    )
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('reg', anon_1.reg, 'count', anon_1.aggr_0) AS value
        FROM
          (SELECT anon_2.reg AS reg,
                  coalesce(anon_3.value, 0) AS aggr_0
           FROM
             (SELECT region_1.name AS reg
              FROM nation AS nation_1
              JOIN region AS region_1 ON nation_1.region_id = region_1.id
              GROUP BY region_1.name) AS anon_2
           LEFT OUTER JOIN
             (SELECT anon_4.reg AS reg,
                     count(*) AS value
              FROM
                (SELECT region_2.name AS reg,
                        nation_1.id AS id,
                        nation_1.name AS name,
                        nation_1.region_id AS region_id,
                        nation_1.comment AS COMMENT
                 FROM nation AS nation_1
                 JOIN region AS region_2 ON nation_1.region_id = region_2.id
                 WHERE nation_1.name = 'KENYA') AS anon_4
              GROUP BY anon_4.reg) AS anon_3 ON anon_2.reg = anon_3.reg) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_group_nation_by_region_select_aggr_array_ok(snapshot):
    query = q.nation.group(reg=q.region.name).select(reg=q.reg, all=q._.name)
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('reg', anon_1.reg, 'all', anon_1.aggr_0) AS value
        FROM
          (SELECT anon_2.reg AS reg,
                  coalesce(anon_3.value, CAST('[]' AS JSONB)) AS aggr_0
           FROM
             (SELECT region_1.name AS reg
              FROM nation AS nation_1
              JOIN region AS region_1 ON nation_1.region_id = region_1.id
              GROUP BY region_1.name) AS anon_2
           LEFT OUTER JOIN
             (SELECT region_2.name AS reg,
                     jsonb_agg(nation_1.name) AS value
              FROM nation AS nation_1
              JOIN region AS region_2 ON nation_1.region_id = region_2.id
              GROUP BY region_2.name) AS anon_3 ON anon_2.reg = anon_3.reg) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_group_then_nav_to_field_ok(snapshot):
    query = q.nation.group(reg=q.region.name).reg
    assert run(query, print_op=True) == n(
        """
        SELECT anon_1.reg AS value
        FROM
          (SELECT region_1.name AS reg
           FROM nation AS nation_1
           JOIN region AS region_1 ON nation_1.region_id = region_1.id
           GROUP BY region_1.name) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_group_ok(snapshot):
    query = q.nation.group(reg=q.region.name)
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('reg', anon_1.reg) AS value
        FROM
          (SELECT region_1.name AS reg
           FROM nation AS nation_1
           JOIN region AS region_1 ON nation_1.region_id = region_1.id
           GROUP BY region_1.name) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_group_select_nav_ok(snapshot):
    query = q.nation.group(n=q.region.name)._.count()
    assert run(query, print_op=True) == n(
        """
        SELECT anon_1.value AS value
        FROM
          (SELECT count(*) AS value
           FROM nation AS nation_1) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_group_select_nav_name_ok(snapshot):
    query = q.nation.group(n=q.region.name)._.name
    assert run(query, print_op=True) == n(
        """
        SELECT nation_1.name AS value
        FROM nation AS nation_1
        """
    )
    assert_result_matches(snapshot, query)


def test_group_select_nav_link_name_ok(snapshot):
    query = q.nation.group(n=q.region.name)._.region.name
    assert run(query, print_op=True) == n(
        """
        SELECT region_1.name AS value
        FROM nation AS nation_1
        JOIN region AS region_1 ON nation_1.region_id = region_1.id
        """
    )
    assert_result_matches(snapshot, query)


def test_group_binop_aggr_ok(snapshot):
    query = q.region.group(len=q.name).select(names=q._.name + "!")
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('names', anon_1.aggr_0) AS value
        FROM
          (SELECT anon_2.len AS LEN,
                  coalesce(anon_3.value, CAST('[]' AS JSONB)) AS aggr_0
           FROM
             (SELECT region_1.name AS LEN
              FROM region AS region_1
              GROUP BY region_1.name) AS anon_2
           LEFT OUTER JOIN
             (SELECT region_1.name AS LEN,
                     jsonb_agg(region_1.name || '!') AS value
              FROM region AS region_1
              GROUP BY region_1.name) AS anon_3 ON anon_2.len = anon_3.len) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_nested_group(snapshot):
    query = q.nation.group(r1=q.name.substring(1, 1)).select(
        r1=q.r1,
        names=q._.name,
        nested=(
            q._.group(r2=q.name.substring(1, 2)).select(
                r2=q.r2, names2=q._.name
            )
        ),
    )
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('r1', anon_1.r1, 'names', anon_1.aggr_0, 'nested', anon_1.aggr_1) AS value
        FROM
          (SELECT anon_2.r1 AS r1,
                  coalesce(anon_3.value, CAST('[]' AS JSONB)) AS aggr_0,
                  coalesce(anon_4.value, CAST('[]' AS JSONB)) AS aggr_1
           FROM
             (SELECT SUBSTRING(nation_1.name
                               FROM 1
                               FOR 1) AS r1
              FROM nation AS nation_1
              GROUP BY SUBSTRING(nation_1.name
                                 FROM 1
                                 FOR 1)) AS anon_2
           LEFT OUTER JOIN
             (SELECT SUBSTRING(nation_1.name
                               FROM 1
                               FOR 1) AS r1,
                     jsonb_agg(nation_1.name) AS value
              FROM nation AS nation_1
              GROUP BY SUBSTRING(nation_1.name
                                 FROM 1
                                 FOR 1)) AS anon_3 ON anon_2.r1 = anon_3.r1
           LEFT OUTER JOIN
             (SELECT anon_5.r1 AS r1,
                     jsonb_agg(jsonb_build_object('r2', anon_5.r2, 'names2', anon_5.aggr_0)) AS value
              FROM
                (SELECT anon_6.r1 AS r1,
                        anon_6.r2 AS r2,
                        coalesce(anon_7.value, CAST('[]' AS JSONB)) AS aggr_0
                 FROM
                   (SELECT SUBSTRING(nation_1.name
                                     FROM 1
                                     FOR 1) AS r1,
                           SUBSTRING(nation_1.name
                                     FROM 1
                                     FOR 2) AS r2
                    FROM nation AS nation_1
                    GROUP BY SUBSTRING(nation_1.name
                                       FROM 1
                                       FOR 1),
                             SUBSTRING(nation_1.name
                                       FROM 1
                                       FOR 2)) AS anon_6
                 LEFT OUTER JOIN
                   (SELECT SUBSTRING(nation_1.name
                                     FROM 1
                                     FOR 1) AS r1,
                           SUBSTRING(nation_1.name
                                     FROM 1
                                     FOR 2) AS r2,
                           jsonb_agg(nation_1.name) AS value
                    FROM nation AS nation_1
                    GROUP BY SUBSTRING(nation_1.name
                                       FROM 1
                                       FOR 1),
                             SUBSTRING(nation_1.name
                                       FROM 1
                                       FOR 2)) AS anon_7 ON anon_6.r1 = anon_7.r1
                 AND anon_6.r2 = anon_7.r2) AS anon_5
              GROUP BY anon_5.r1) AS anon_4 ON anon_3.r1 = anon_4.r1) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_substring_rel_ok(snapshot):
    query = q.region.name.substring(1, 2)
    assert run(query, print_op=True) == n(
        """
        SELECT SUBSTRING(region_1.name
                         FROM 1
                         FOR 2) AS value
        FROM region AS region_1
        """
    )
    assert_result_matches(snapshot, query)


def test_substring_expr_ok(snapshot):
    query = q.region.select(silly_abbr=q.name.substring(1, 2))
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('silly_abbr', SUBSTRING(region_1.name
                                                          FROM 1
                                                          FOR 2)) AS value
        FROM region AS region_1
        """
    )
    assert_result_matches(snapshot, query)


def test_substring_rel_non_expr_ok(snapshot):
    query = q.region.name.take(2).substring(1, 2)
    assert run(query, print_op=True) == n(
        """
        SELECT SUBSTRING(anon_1.name
                         FROM 1
                         FOR 2) AS value
        FROM
          (SELECT region_1.id AS id,
                  region_1.name AS name,
                  region_1.comment AS COMMENT
           FROM region AS region_1
           LIMIT 2) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


def test_group_by_link_ok(snapshot):
    query = q.nation.group(n=q.region)
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('n', anon_1.n) AS value
        FROM
          (SELECT CAST(row(region_1.name) AS VARCHAR) AS n
           FROM nation AS nation_1
           JOIN region AS region_1 ON nation_1.region_id = region_1.id
           GROUP BY CAST(row(region_1.name) AS VARCHAR)) AS anon_1
        """
    )
    assert_result_matches(snapshot, query)


@pytest.mark.xfail
def test_group_nav_after_take(snapshot):
    query = q.region.group(n=q.name).take(1)._
    assert run(query, print_op=True) == n(
        """
        """
    )
    assert_result_matches(snapshot, query)


@pytest.mark.xfail
def test_group_group_with_take(snapshot):
    query = q.region.group(n=q.name).select(nation=q._.nation.take(1))
    assert run(query, print_op=True) == n(
        """
        """
    )
    assert_result_matches(snapshot, query)


def test_fork_ok(snapshot):
    query = q.region.filter(q.name.substring(1, 1) == "A").select(
        n=q.name, nn=q.fork().count()
    )
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('n', anon_1.name, 'nn', anon_2.value) AS value
        FROM
          (SELECT region_1.id AS id,
                  region_1.name AS name,
                  region_1.comment AS COMMENT
           FROM region AS region_1
           WHERE SUBSTRING(region_1.name
                           FROM 1
                           FOR 1) = 'A') AS anon_1
        LEFT OUTER JOIN LATERAL
          (SELECT count(*) AS value
           FROM
             (SELECT region_1.id AS id,
                     region_1.name AS name,
                     region_1.comment AS COMMENT
              FROM region AS region_1
              WHERE SUBSTRING(region_1.name
                              FROM 1
                              FOR 1) = 'A') AS anon_1) AS anon_2 ON TRUE
        """
    )


def test_fork_through_ok(snapshot):
    query = q.nation.select(
        n=q.name,
        nn=q.fork(q.region).filter(q.region.name == "EUROPE").count(),
    )
    assert run(query, print_op=True) == n(
        """
        SELECT jsonb_build_object('n', nation_1.name, 'nn', anon_1.value) AS value
        FROM nation AS nation_1
        LEFT OUTER JOIN LATERAL
          (SELECT count(*) AS value
           FROM
             (SELECT nation_2.id AS id,
                     nation_2.name AS name,
                     nation_2.region_id AS region_id,
                     nation_2.comment AS COMMENT
              FROM
                (SELECT region.id AS id,
                        region.name AS name,
                        region.comment AS COMMENT
                 FROM region
                 WHERE region.id = nation_1.region_id) AS anon_3
              JOIN nation AS nation_2 ON anon_3.id = nation_2.region_id
              JOIN region AS region_1 ON nation_2.region_id = region_1.id
              WHERE region_1.name = 'EUROPE') AS anon_2) AS anon_1 ON TRUE
        """
    )
