import click


@click.command()
@click.argument("db", default="postgresql://")
def shell(db):
    from IPython.terminal.embed import embed
    from sqlalchemy import create_engine, MetaData
    from qc0 import Q as BaseQ, execute

    engine = create_engine(db)
    meta = MetaData()
    meta.reflect(bind=engine)

    class Q(BaseQ):
        def run(self):
            return execute(self, meta, engine)

    q = Q(None)  # NOQA

    embed()
