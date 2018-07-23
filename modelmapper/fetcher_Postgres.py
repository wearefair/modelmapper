from modelmapper.fetcher import Fetcher

try:
    from sqlalchemy.dialects.postgresql import insert
except ImportError:
    def insert(table):
        raise ImportError('Please install SQLAlchemy')


class PostgresFetcher(Fetcher):

    def insert_chunk_of_data_to_db(self, session, table, chunk):
        new_chunk = list(self.add_row_signature(chunk)) if self.settings.ignore_duplicate_rows_when_importing else list(chunk)  # NOQA
        insrt_stmnt = insert(table).values(new_chunk)
        do_nothing_stmt = insrt_stmnt.on_conflict_do_nothing()
        results = session.execute(do_nothing_stmt)
        return results.rowcount
