import pytest
import decimal
import datetime
import json
import sqlalchemy as sa
import qc0


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o.as_tuple().exponent == 0:
                return int(o)
            else:
                return float(o)
        if isinstance(o, datetime.date):
            return o.strftime("%Y-%m-%d")
        return super(JSONEncoder, self).default(o)


class Q(qc0.Q):
    def run(self):
        res = super(Q, self).run()
        return json.loads(json.dumps(res, cls=JSONEncoder, sort_keys=True))


engine = sa.create_engine("postgresql://")
meta = sa.MetaData()
meta.reflect(bind=engine)
q = Q(meta=meta, engine=engine)


def execute_sql(sql):
    r = engine.execute(sql)
    rows = r.fetchall()
    return json.loads(
        json.dumps(
            [dict(r.items()) for r in rows], cls=JSONEncoder, sort_keys=True
        )
    )


@pytest.fixture(autouse=True)
def add_np(doctest_namespace):
    doctest_namespace["q"] = q
    doctest_namespace["execute_sql"] = execute_sql
    doctest_namespace["date"] = datetime.date
