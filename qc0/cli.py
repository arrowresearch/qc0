import click


@click.command()
@click.argument("db", default="postgresql://")
def shell(db):
    from IPython.terminal.embed import embed
    from sqlalchemy import create_engine, MetaData
    from qc0 import Q as BaseQ, bind, compile

    engine = create_engine(db)
    meta = MetaData()
    meta.reflect(bind=engine)

    class Q(BaseQ):
        def run(self):
            op = bind(self.syn, meta)
            sql = compile(op)
            with engine.connect() as conn:
                res = conn.execute(sql)
                return [row[0] for row in res.fetchall()]

    q = Q(None)  # NOQA

    embed()
