import click


@click.command()
@click.argument("db", default="postgresql://")
def shell(db):
    from IPython.terminal.embed import embed
    from sqlalchemy import create_engine, MetaData
    from qc0 import Q as BaseQ, execute, syn_to_op, op_to_sql

    engine = create_engine(db)
    meta = MetaData()
    meta.reflect(bind=engine)

    class Q(BaseQ):
        def op(self):
            op = syn_to_op(self.syn, meta)
            print(op)

        def sql(self):
            op = syn_to_op(self.syn, meta)
            sql = op_to_sql(op)
            sql = sql.compile(engine, compile_kwargs={"literal_binds": True})
            print(sql)

        def run(self):
            return execute(self, meta, engine)

    q = Q(None)  # NOQA

    embed()
