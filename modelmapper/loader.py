from modelmapper.signature import generate_row_signature
try:
    from sqlalchemy.dialects.postgresql import insert
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.sql import select
except ImportError:
    def insert(table):
        raise ImportError('Please install SQLAlchemy')

    def select(fields):
        raise ImportError('Please install SQLAlchemy')


class BaseLoaderMixin():
    """
    Base class for loaders. Completely db and data structure agnostic.
    REQUIRED: Override insert_row_into_db
    """
    def pre_row_insert(self, row: dict, session, model):
        """Override to add any logic for rows before being loaded"""
        return row

    def post_row_insert(self, row, session, model):
        """Override to add any logic for rows after being loaded"""
        pass

    def insert_row_into_db(self, row: dict, session, model):
        """REQUIRED IMPLEMENTATION for how a row is loaded into the db"""
        raise NotImplementedError("Please implement the insert_row_into_db.")

    def insert_chunk_of_data_to_db(self, session, model, chunk=()):
        """Processing chunks of rows to be loaded into db"""
        count = 0
        for row in chunk:
            row = self.pre_row_insert(row, session, model)
            if row:
                inserted_row = self.insert_row_into_db(row, session, model)
                self.post_row_insert(inserted_row, session, model)
                count += 1
        return count


class SqlalchemyLoaderMixin(BaseLoaderMixin):
    """
    Simple loader for inserting into a db with sqlalchemy.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._fail_on_integrity_error = False

    def insert_row_into_db(self, row: dict, session, model):
        """Insert row into db"""
        row_obj = model(**row)
        try:
            session.add(row_obj)
            session.flush()
            return row_obj
        except IntegrityError:
            self.logger.debug(f"Row caused Integrity Error: {row}")
            if self._fail_on_integrity_error:
                raise
        except Exception:
            self.logger.exception(f"Error on inserting row of data: {row}")
            raise


class SignatureSqlalchemyMixin(SqlalchemyLoaderMixin):
    """
    Base Signature loader. A signature column will be added to each row whose value is a 64-bit
    murmur hash of the row (requiring BigInteger type). This will insert DUPLICATE ROWS!!!
    If unique rows are desired, use the: UniqueSignatureSqlalchemyLoaderMixin.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.all_recent_rows_signatures = set()

    def add_row_signature(self, chunk):
        """Add hash of row to row about to be inserted"""
        for row in chunk:
            row['signature'] = generate_row_signature(
                row, self.RECORDS_MODEL, self.settings.ignore_fields_in_signature_calculation
            )
            yield row

    def get_id_by_signature(self, session, model, signature):
        """Searches given table for given signature. Returns row if found or None if not"""
        table = model.__table__
        query = select([table.c.id]).where(table.c.signature == signature)
        query_result = session.execute(query)
        result = query_result.fetchone()
        result = None if result is None else result[0]
        return result

    def insert_chunk_of_data_to_db(self, session, model, chunk):
        """Add row signature to row then run Base class logic"""
        new_chunk = self.add_row_signature(chunk)
        return super().insert_chunk_of_data_to_db(session, model, new_chunk)


class SqlalchemySnapshotLoaderMixin(SignatureSqlalchemyMixin):
    """
    Sqlalchemy Specific Loader with Snapshot Auxilary Table for row dupes.
    It adds the records to the snapshot model.
    """
    SNAPSHOT_MODEL = None

    def insert_chunk_of_data_to_db(self, session, model, chunk):
        table = model.__table__
        snapshot_table = self.SNAPSHOT_MODEL.__table__
        new_chunk = self.add_row_signature(chunk) if self.settings.ignore_duplicate_rows_when_importing else chunk  # NOQA
        count = 0
        if new_chunk:
            for row in new_chunk:
                id_ = None
                if row['signature']:
                    id_ = self.get_id_by_signature(session, model, row['signature'])
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


class SqlalchemyBulkLoaderMixin():
    """
    Sqlalchemy Specific Bulk Loader.
    Handles Bulk inserts and ignores when there were errors and continues
    """

    def add_row_signature(self, chunk):
        for row in chunk:
            row['signature'] = signature = generate_row_signature(
                row, self.RECORDS_MODEL, self.settings.ignore_fields_in_signature_calculation
            )
            if signature and signature not in self.all_recent_rows_signatures:
                self.all_recent_rows_signatures.add(signature)
                yield row

    def insert_chunk_of_data_to_db(self, session, model, chunk):
        table = model.__table__
        new_chunk = list(self.add_row_signature(chunk)) if self.settings.ignore_duplicate_rows_when_importing else list(chunk)  # NOQA
        if new_chunk:
            insrt_stmnt = insert(table).values(new_chunk)
            do_nothing_stmt = insrt_stmnt.on_conflict_do_nothing()
            results = session.execute(do_nothing_stmt)
            return results.rowcount
        else:
            return 0
