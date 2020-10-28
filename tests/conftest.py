import pytest
import sqlalchemy as sa
import qc0

engine = sa.create_engine("postgresql://")
meta = sa.MetaData()
meta.reflect(bind=engine)
q = qc0.Q(meta=meta, engine=engine)


@pytest.fixture(autouse=True)
def add_np(doctest_namespace):
    doctest_namespace["q"] = q
