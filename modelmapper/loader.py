try:
    from sqlalchemy.dialects.postgresql import insert
    from sqlalchemy.sql import select
except ImportError:
    def insert(table):
        raise ImportError('Please install SQLAlchemy')

    def select(fields):
        raise ImportError('Please install SQLAlchemy')


class BaseLoaderMixin():
    """
    Base class for loaders.
    """
    def pre_row_insert(self, row, session, table):
        """Override to add any logic for rows before being loaded"""
        return row

    def post_row_insert(self, row, session, table):
        """Override to add any logic for rows after being loaded"""
        pass

    def insert_row_into_db(self, row, session, table):
        """REQUIRED IMPLEMENTATION for how a row is loaded into the db"""
        raise NotImplementedError("Please implement the insert_row_into_db.")

    def insert_chunk_of_data_to_db(self, session, table, chunk):
        """Processing chunks of rows to be loaded into db"""
        count = 0
        if chunk:
            for row in chunk:
                if row is None:
                    continue
                row = self.pre_row_insert(row, session, table)
                if row is not None:
                    self.insert_row_into_db(row, session, table)
                    self.post_row_insert(row, session, table)
                    count += 1
        return count


class SimpleLoaderMixin(BaseLoaderMixin):
    def insert_row_into_db(self, row, session, table):
        """Insert row into db"""
        ins = table.insert().values(**row)
        try:
            session.execute(ins)
            session.flush()
        except Exception as e:
            self.logger.error(f"Error on inserting row of data: {row}")
            raise e


class BaseSignatureMixin(BaseLoaderMixin):

    def get_id_by_signature(self, session, table, signature):
        """Searches given table for given signature. Returns row if found or None if not"""
        query = select([table.c.id]).where(table.c.signature == signature)
        query_result = session.execute(query)
        result = query_result.fetchone()
        result = None if result is None else result[0]
        return result

    def insert_chunk_of_data_to_db(self, session, table, chunk):
        """Add row signature to row then run Base class logic"""
        new_chunk = self.add_row_signature(chunk)
        return super(BaseSignatureMixin, self).insert_chunk_of_data_to_db(session, table, new_chunk)


class UniqueSignatureLoaderMixin(BaseSignatureMixin):
    """
    Postgres Specific Loader that backs up raw data into S3.
    It appends a signature to the row only uploads if it is not already in the table.
    Requires add_row_signature() method to be defined in the class.
    """

    def backup_data(self, content, key, metadata):
        self.put_file_on_s3(content=content, key=key, metadata=metadata)

    def insert_row_into_db(self, row, session, table):
        """Insert row only if the signature is unique to the table"""
        if row['signature'] and not self.get_id_by_signature(session, table, row['signature']):
            ins = table.insert().values(**row)
            try:
                session.execute(ins)
                session.flush()
            except Exception as e:
                self.logger.error(f"Error on inserting row of data: {row}")
                raise e


class DuplicateSignatureLoaderMixin(BaseSignatureMixin):
    """
    Loader that backs up raw data into S3.
    It appends a signature to the row and loads it into the db.
    Requires add_row_signature() method to be defined in the class.
    """

    def backup_data(self, content, key, metadata):
        self.put_file_on_s3(content=content, key=key, metadata=metadata)

    def insert_row_into_db(self, row, session, table):
        """Insert row into the table"""
        ins = table.insert().values(**row)
        try:
            session.execute(ins)
            session.flush()
        except Exception as e:
            self.logger.error(f"Error on inserting row of data: {row}")
            raise e


class PostgresSnapshotLoaderMixin(BaseSignatureMixin):
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
                    id_ = self.get_id_by_signature(session, table, row['signature'])
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