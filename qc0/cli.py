import click
from datetime import date  # noqa: F401


@click.command()
@click.argument("db", default="postgresql://")
def shell(db):
    from IPython.terminal.embed import embed
    from sqlalchemy import create_engine, MetaData
    from qc0 import Q

    engine = create_engine(db)
    meta = MetaData()
    meta.reflect(bind=engine)
    q = Q(meta=meta, engine=engine)  # NOQA

    embed()
