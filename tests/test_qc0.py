from textwrap import dedent
from sqlalchemy import create_engine, MetaData
from qc import q, bind, compile

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
        SELECT nation.id, nation.name, nation.region_id, nation.comment
        FROM nation AS nation
        """
    )


def test_nav_nation_name():
    assert run(q.nation.name) == n(
        """
        SELECT nation.name AS value
        FROM nation AS nation
        """
    )


def test_nav_nation_region_name():
    assert run(q.nation.region.name) == n(
        """
        SELECT region.name AS value
        FROM nation AS nation JOIN region AS region ON region.id = nation.region_id
        """
    )


def test_select_nav_select():
    assert run(q.nation.select(name=q.name, comment=q.comment)) == n(
        """
        SELECT json_build_object('name', nation.name, 'comment', nation.comment) AS value
        FROM nation AS nation
        """
    )


def test_select_nav_select_nav_only():
    assert run(q.nation.select(region_name=q.region.name)) == n(
        """
        SELECT json_build_object('region_name', region.name) AS value
        FROM nation AS nation JOIN region AS region ON region.id = nation.region_id
        """
    )


def test_select_nav_select_nav_multi():
    assert run(
        q.nation.select(
            name=q.name,
            region_name=q.region.name,
            region_comment=q.region.comment,
        )
    ) == n(
        """
        SELECT json_build_object('name', nation.name, 'region_name', region.name, 'region_comment', region.comment) AS value
        FROM nation AS nation JOIN region AS region ON region.id = nation.region_id
        """
    )


def test_select_nav_select_nav_select():
    assert run(q.nation.select(region=q.region.select(name=q.name))) == n(
        """
        SELECT json_build_object('region', json_build_object('name', region.name)) AS value
        FROM nation AS nation JOIN region AS region ON region.id = nation.region_id
        """
    )


def test_select_select_nav():
    assert run(q.select(region_name=q.region.name), print_op=True) == n(
        """
        SELECT json_build_object('region', json_build_object('name', region.name)) AS value
        FROM nation AS nation JOIN region AS region ON region.id = nation.region_id
        """
    )
