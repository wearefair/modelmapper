try:
    import mmh3
except ImportError:
    class mmh3:
        def hash(self, *args, **kwargs):
            raise ImportError('Please install mmh3')

import gzip
import datetime
import logging
import pickle

from collections import Mapping


from modelmapper.base import Base
from modelmapper.cleaner import Cleaner
from modelmapper.slack import slack
from modelmapper.misc import generator_chunker, generator_updater
from modelmapper.s3 import S3Mixin

try:
    from sqlalchemy.dialects.postgresql import insert
    from sqlalchemy.sql import select
except ImportError:
    def insert(table):
        raise ImportError('Please install SQLAlchemy')

    def select(fields):
        raise ImportError('Please install SQLAlchemy')


class Loader(Base):
    """
    Subclass this for your data processing and define the BUCKET_NAME, RAW_KEY_MODEL and RECORDS_MODEL.

    """

    BUCKET_NAME = None
    RAW_KEY_MODEL = None
    RECORDS_MODEL = None
    BACKUP_KEY_DATETIME_FORMAT = '%Y/%m/%Y_%m_%d__%H_%M_%S.gzip'
    SQL_CHUNK_ROWS = 300
    logger = logging.getLogger(__name__)

    def __init__(self, *args, **kwargs):
        self.JOB_NAME = self.__class__.__name__
        self.DUMP_FILEPATH = f'/tmp/{self.JOB_NAME}_dump'
        super().__init__(*args, **kwargs)
        kwargs['setup_path'] = self.setup_path
        self.cleaner = Cleaner(*args, **kwargs)

    def get_client_data(self):
        """
        The client should return data that the importer will clean and import.
        """
        raise NotImplementedError('Please implement the get_client_data in your subclass.')

    def report_exception(self, e):
        raise NotImplementedError('Please implement this method in your subclass.')

    def get_hash_of_bytes(self, item):
        return mmh3.hash(item)

    def get_hash_of_row(self, row):
        if isinstance(row, list):
            items = row
        elif isinstance(row, Mapping):
            items = row.items()
        row_bytes = b','.join([str(v).encode('utf-8') for k, v in items if k not in self.settings.ignore_fields_in_signature_calculation])  # NOQA
        return self.get_hash_of_bytes(row_bytes)

    def report_error_to_slack(self, e, msg='{} The {} failed: {}'):
        slack_handle_to_ping = f'<{self.settings.slack_handle_to_ping}>' if self.settings.slack_handle_to_ping else''
        try:
            msg = msg.format(slack_handle_to_ping, self.JOB_NAME, e)
        except Exception as e:
            pass
        self.slack(msg)

    def slack(self, text):
        return slack(text,
                     username=self.settings.slack_username,
                     channel=self.settings.slack_channel,
                     slack_http_endpoint=self.settings.slack_http_endpoint)

    def _compress(data):
        if isinstance(data, (dict, list, tuple)):
            data = json.dumps(data).encode('utf-8')
        elif isinstance(data, str):
            data = data.encode('utf-8')

        if isinstance(data, bytes):
            data = gzip.compress(data)
        else:
            raise TypeError(f'Data format of {data.JOB_NAME} failed to turn into bytes for compression')
        return data

    def encrypt_data(self, data):
        raise NotImplementedError('Please implement encrypt_data method')

    def backup_data(self, content, key, metadata):
        raise NotImplementedError('Please implement backup_data method')

    def _backup_data_and_get_raw_key(self, session, data_raw_bytes):
        key = datetime.datetime.strftime(datetime.datetime.utcnow(), self.BACKUP_KEY_DATETIME_FORMAT)
        signature = mmh3.hash(data_raw_bytes)
        raw_key_id = self._create_raw_key(session, key, signature)
        data_compressed = self._compress(data_raw_bytes)
        if self.settings.encrypt_raw_data_during_backup:
            data_compressed = self.encrypt_data(data_compressed)
        self.logger.info(f'Backing up the data into s3 bucket: {key}')
        metadata = {'compression': 'gzip'}
        self.backup_data(content=data_compressed, key=key, metadata=metadata)
        return raw_key_id

    def _dump_state_after_client_response(self, data):
        with open(f'{self.DUMP_FILEPATH}.pickle', "wb") as the_file:
            pickle.dump(data, the_file)

    def _load_state_after_client_response(self):
        with open(f'{self.DUMP_FILEPATH}.pickle', "rb") as the_file:
            return pickle.load(the_file)

    def _create_raw_key(self, session, key, signature):
        try:
            raw_key = self.RAW_KEY_MODEL(key=key, signature=signature)
            session.add(raw_key)
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        return raw_key.id

    def add_row_signature(self, chunk):
        for row in chunk:
            row['signature'] = signature = self.get_row_signature(row)
            if signature and signature not in self.all_recent_rows_signatures:
                self.all_recent_rows_signatures.add(signature)
                yield row

    def get_row_signature(self, row):
        sorted_row = sorted(row.items(), key=lambda t: t[0])
        return self.get_hash_of_row(sorted_row)

    def get_session(self):
        raise NotImplementedError('Please provide a function for SQLAlchemy session.')

    def insert_chunk_of_data_to_db(self, session, table, chunk):
        raise NotImplementedError('Please implement this method based on your database engine.')

    def insert_row_data_to_db(self, session, data_gen, row_metadata):
        self.logger.info(f"{self.JOB_NAME}: Inserting data into db")
        table = self.RECORDS_MODEL.__table__

        row_count = 0

        self.all_recent_rows_signatures = set()

        data_gen = generator_updater(data_gen, **row_metadata)
        chunks = generator_chunker(data_gen, chunk_size=self.SQL_CHUNK_ROWS)
        try:
            for chunk in chunks:
                chunk_rows_inserted = self.insert_chunk_of_data_to_db(session, table, chunk)
                row_count += chunk_rows_inserted
                if chunk_rows_inserted:
                    self.logger.debug(f'{self.JOB_NAME}: Put {row_count} rows in the database.')
        except Exception as e:
            self.logger.exception(f'Error when inserting row into -: {e}')
            try:
                session.rollback()
            except Exception as e2:
                self.logger.error('Failed to rollback the transaction: {}'.format(e2))
        else:
            if row_count:
                msg = f'{self.JOB_NAME}: Finished putting {row_count} rows in the database.'
            else:
                msg = f'{self.JOB_NAME}: There is no new data to be put in the database.'
            self.logger.info(msg)
            session.commit()

    def encrypt_row_fields(self, cleaned_data_gen):
        raise NotImplementedError('Please implement encrypt_row_fields')

    def verify_access_to_backup_source(self):
        raise NotImplementedError('Please implement verify_access_to_backup_source')

    def do_fetch(self, session, path=None, content=None, content_type=None,
                 sheet_names=None, use_client=True, backup_data=True):
        invalid_choices = [
            (path is None and content is None and not use_client,
             ValueError('If path and content are None, the client must be used.')),
            ((path or content) is not None and use_client,
             ValueError('If path or content are defined, the client should not be used.')),
            (not use_client and backup_data,
             ValueError('Data can be only backed up if the client is used.'))
        ]

        for case, err in invalid_choices:
            if case:
                raise err

        if backup_data:
            self.verify_access_to_backup_source()

        self.logger.info(f'Starting the {self.JOB_NAME} ...')

        if use_client:
            content = self.get_client_data()

        if backup_data:
            content = content.encode('utf-8') if isinstance(content, str) else content
            raw_key_id = self._backup_data_and_get_raw_key(session, data_raw_bytes=content)
        else:
            raw_key_id = self._create_raw_key(session, key=path, signature=None)

        data = {"content": content, "raw_key_id": raw_key_id, "content_type": content_type,
                "path": path, "sheet_names": sheet_names}
        self._dump_state_after_client_response(data)
        return data

    def do_load(self, session, data):

        cleaned_data_gen = self.cleaner.clean(content_type=data['content_type'], path=data['path'],
                                              content=data['content'], sheet_names=data['sheet_names'])

        if self.settings.fields_to_be_encrypted:
            cleaned_data_gen = self.encrypt_row_fields(cleaned_data_gen)

        row_metadata = {'raw_key_id': data['raw_key_id']}
        self.insert_row_data_to_db(session, data_gen=cleaned_data_gen, row_metadata=row_metadata)

    def load(self, ping_slack=False, path=None, content=None, content_type=None,
             sheet_names=None, use_client=True, backup_data=True):
        try:
            with self.get_session() as session:
                data = self.do_fetch(session, path=path, content=content, content_type=content_type,
                                     sheet_names=sheet_names, use_client=use_client, backup_data=backup_data)
                self.do_load(session, data)
        except Exception as e:
            self.report_exception(e)
            self.logger.exception(str(e))
            if ping_slack:
                self.report_error_to_slack(e)

    def reload(self):
        """
        Reload from pickle dump of the last run of the client.
        Meant for local usage only.
        """
        data = self._load_state_after_client_response()
        with self.get_session() as session:
            self.do_load(session, data)


class PostgresBulkLoader(Loader, S3Mixin):
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


class PostgresSnapshotLoader(Loader, S3Mixin):
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
