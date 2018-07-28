try:
    from sqlalchemy.dialects.postgresql import insert
    from sqlalchemy.sql import select
except ImportError:
    def insert(table):
        raise ImportError('Please install SQLAlchemy')

    def select(fields):
        raise ImportError('Please install SQLAlchemy')


class PostgresBulkLoaderMixin():
    """
    Postgres Specific Loader that backs up raw data into S3.
    Handles Bulk inserts and ignores when there were errors and continues
    """

    def backup_data(self, content, key, metadata):
        self.put_file_on_s3(content=content, key=key, metadata=metadata)

    def insert_chunk_of_data_to_db(self, session, table, chunk):
        new_chunk = list(self.add_row_signature(chunk)) if self.settings.ignore_duplicate_rows_when_importing else list(chunk)  # NOQA
        if new_chunk:
            insrt_stmnt = insert(table).values(new_chunk)
            do_nothing_stmt = insrt_stmnt.on_conflict_do_nothing()
            results = session.execute(do_nothing_stmt)
            return results.rowcount
        else:
            return 0


def get_id_by_signature(session, table, signature):
    query = select([table.c.id]).where(table.c.signature == signature)
    query_result = session.execute(query)
    result = query_result.fetchone()
    result = None if result is None else result[0]
    return result


class PostgresSnapshotLoaderMixin():
    """
    Postgres Specific Loader that backs up raw data into S3.
    It adds the records to the snapshot model.
    """
    SNAPSHOT_MODEL = None

    def backup_data(self, content, key, metadata):
        self.put_file_on_s3(content=content, key=key, metadata=metadata)

    def insert_chunk_of_data_to_db(self, session, table, chunk):
        snapshot_table = self.SNAPSHOT_MODEL.__table__
        new_chunk = self.add_row_signature(chunk) if self.settings.ignore_duplicate_rows_when_importing else chunk  # NOQA
        count = 0
        if new_chunk:
            for row in new_chunk:
                id_ = None
                if row['signature']:
                    id_ = get_id_by_signature(session, table, row['signature'])
                if not id_:
                    ins = table.insert().values(**row)
                    result = session.execute(ins)
                    session.flush()
                    id_ = result.inserted_primary_key[0]
                    count += 1
                if id_:
                    snapshot_row = {'raw_key_id': row['raw_key_id'], 'record_id': id_}
                    ins = snapshot_table.insert().values(**snapshot_row)
                    result = session.execute(ins)
            session.flush()
        return count
