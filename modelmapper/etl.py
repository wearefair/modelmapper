import gzip
import datetime
import logging
import pickle
import types
import json

from modelmapper.base import Base
from modelmapper.cleaner import Cleaner, CastingError
from modelmapper.misc import generator_chunker, generator_updater
from modelmapper.signature import get_hash_of_bytes
from modelmapper.exceptions import NothingToProcess, FileAlreadyProcessed
from sqlalchemy import exc as core_exc


class ETL(Base):
    """
    Subclass this for your data processing and define the BUCKET_NAME, RAW_KEY_MODEL and RECORDS_MODEL.
    """

    RAW_KEY_MODEL = None
    RECORDS_MODEL = None
    BACKUP_KEY_DATETIME_FORMAT = '%Y/%m/%Y_%m_%d__%H_%M_%S.gzip'
    SQL_CHUNK_ROWS = 300
    SIGNATURE_BITS = 128
    logger = logging.getLogger(__name__)

    def __init__(self, *args, **kwargs):
        self.JOB_NAME = self.__class__.__name__
        self.DUMP_FILEPATH = f'/tmp/{self.JOB_NAME}_dump'
        super().__init__(*args, **kwargs)
        kwargs['setup_path'] = self.setup_path
        self.cleaner = Cleaner(*args, **kwargs)
        self.backup_key_name = None

    def get_client_data(self):
        """
        The client should return data that the importer will clean and import.
        """
        raise NotImplementedError('Please implement the get_client_data in your subclass.')

    def report_exception(self, e, extra=None):
        raise NotImplementedError('Please implement this method in your subclass.')

    def report_error_to_slack(self, e, msg='{} The {} failed: {} backup_key_name: {}'):
        slack_handle_to_ping = f'<{self.settings.slack_handle_to_ping}>' if self.settings.slack_handle_to_ping else''
        try:
            msg = msg.format(slack_handle_to_ping, self.JOB_NAME, e, self.backup_key_name)
        except Exception:
            pass
        self.slack(msg)

    def _compress(self, data):
        if isinstance(data, (dict, list, tuple)):
            data = json.dumps(data).encode('utf-8')
        elif isinstance(data, str):
            data = data.encode('utf-8')

        if isinstance(data, bytes):
            data = gzip.compress(data)
        else:
            raise TypeError(f'Data format of {data.JOB_NAME} failed to turn into bytes for compression')
        return data

    def encrypt_raw_data(self, data):
        raise NotImplementedError('Please implement encrypt_data method')

    def backup_data(self, content, key, metadata):
        raise NotImplementedError('Please implement backup_data method')

    def _backup_data_and_get_raw_key(self, session, data_raw_bytes, signature):
        key = datetime.datetime.strftime(datetime.datetime.utcnow(), self.BACKUP_KEY_DATETIME_FORMAT)
        raw_key_id = self._create_raw_key(session, key, signature)
        data_compressed = self._compress(data_raw_bytes)
        if self.settings.encrypt_raw_data_during_backup:
            data_compressed = self.encrypt_raw_data(data_compressed)
        self.logger.info(f'Backing up the data: {key}')
        self.backup_key_name = key
        metadata = {'compression': 'gzip'}
        self.backup_data(content=data_compressed, key=key, metadata=metadata)
        return raw_key_id

    def _dump_state_after_client_response(self, data):
        with open(f'{self.DUMP_FILEPATH}.pickle', "wb") as the_file:
            if isinstance(data['content'], types.GeneratorType):
                # Since the type of data's content is a CSV we can unwrap the
                # generator with list and just take it's first entry.
                data['content'] = next(data['content'])

            pickle.dump(data, the_file)

    def _load_state_after_client_response(self):
        with open(f'{self.DUMP_FILEPATH}.pickle', "rb") as the_file:
            return pickle.load(the_file)

    def _create_raw_key(self, session, key, signature):
        try:
            raw_key = self.RAW_KEY_MODEL(key=key, signature=signature)
            session.add(raw_key)
            session.flush()
        except core_exc.IntegrityError as e:
            session.rollback()
            # We are attempting to process an existing file.
            if self.settings.should_reprocess:
                raw_key = session.query(
                    self.RAW_KEY_MODEL
                ).filter(
                    self.RAW_KEY_MODEL.signature == signature
                ).one()  # <-- raise exception if not found

                self.logger.info(f'Reprocessing for ID: {raw_key.id} ' +
                                 f'[Signature: {self.RAW_KEY_MODEL.signature}]')

                return raw_key.id
            else:
                msg = 'Duplicate File. Set should_reprocess to true in the settings to reprocess it: {e}'
                raise FileAlreadyProcessed(msg)

        except Exception:
            session.rollback()
            raise
        return raw_key.id

    def get_session(self):
        raise NotImplementedError('Please provide a function for SQLAlchemy session.')

    def insert_chunk_of_data_to_db(self, session, table, chunk):
        raise NotImplementedError('Please implement this method based on your database engine.')

    def encrypt_row_fields(self, cleaned_data_gen):
        raise NotImplementedError('Please implement encrypt_row_fields')

    def verify_access_to_backup_source(self):
        raise NotImplementedError('Please implement verify_access_to_backup_source')

    def decrypt_raw_data(self, content):
        raise NotImplementedError('Please implement decrypt_raw_data')

    def _extract(self, session, path=None, content=None, content_type=None,
                 sheet_names=None, use_client=True, backup_data=True,
                 cache_client_response=True):
        invalid_choices = [
            (path is None and content is None and not use_client,
             ValueError('If path and content are None, the client must be used.')),
            ((path or content) is not None and use_client,
             ValueError('If path or content are defined, the client should not be used.')),
            (not use_client and backup_data,
             ValueError('Data can be only backed up if the client is used.'))
        ]
        key = None

        for case, err in invalid_choices:
            if case:
                raise err

        if backup_data:
            self.verify_access_to_backup_source()

        self.logger.info(f'Starting the {self.JOB_NAME} ...')

        if use_client:
            content = self.get_client_data()

        # get_client_data may have returned a key for the raw_key value
        if isinstance(content, tuple):
            content, key = content
        else:
            key = path if path else f'content.{content_type}'

        if isinstance(content, types.GeneratorType):
            content = '\n'.join(content)
        if isinstance(content, str):
            data_raw_bytes = content.encode('utf-8')
        elif isinstance(content, bytes):
            data_raw_bytes = content
        else:
            raise TypeError('Unexpected type of content is received. '
                            'Please make sure the content is either string, bytes or generator.')
        signature = get_hash_of_bytes(data_raw_bytes, bits=self.SIGNATURE_BITS)
        if backup_data:
            try:
                raw_key_id = self._backup_data_and_get_raw_key(
                    session, data_raw_bytes=data_raw_bytes, signature=signature)
            except FileAlreadyProcessed:
                if hasattr(self, 'post_pickup_cleanup'):
                    self.post_pickup_cleanup()
                raise
            else:
                if hasattr(self, 'post_pickup_cleanup'):
                    self.post_pickup_cleanup()
        else:
            raw_key_id = self._create_raw_key(session, key=key, signature=signature)

        if self.settings.decrypt_raw_data:
            content = self.decrypt_raw_data(content)

        data = {"content": content, "raw_key_id": raw_key_id, "content_type": content_type,
                "path": path, "sheet_names": sheet_names}

        if cache_client_response:
            self._dump_state_after_client_response(data)

        return data

    def transform(self, session=None, data_gen=None):
        """
        The function to add your additional transform functionality
        """
        return data_gen

    def transform_raw_content(self, data=None):
        """
        Transforms the data dictionary with raw content
        """
        pass

    def _transform(self, session, data):
        self.transform_raw_content(data)
        data_gen = self.cleaner.clean(content_type=data['content_type'], path=data['path'],
                                      content=data['content'], sheet_names=data['sheet_names'])

        if self.settings.fields_to_be_encrypted:
            data_gen = self.encrypt_row_fields(data_gen)

        row_metadata = {'raw_key_id': data['raw_key_id']}
        data_gen = generator_updater(data_gen, **row_metadata)
        data_gen = self.transform(session, data_gen)

        return data_gen

    def _load(self, session, data_gen):
        self.logger.info(f"{self.JOB_NAME}: Inserting data into db")
        table = self.RECORDS_MODEL.__table__

        row_count = existing_row_count = 0

        self.all_recent_rows_signatures = set()
        chunks = generator_chunker(data_gen, chunk_size=self.SQL_CHUNK_ROWS)
        if chunks is None:
            self.logger.error("No data was provided by generator for table: {}".format(table))
            return
        try:
            for chunk in chunks:
                chunk_rows_inserted, chunk_rows_already_existing = self.insert_chunk_of_data_to_db(
                    session, self.RECORDS_MODEL, chunk)
                if chunk_rows_inserted:
                    row_count += chunk_rows_inserted
                    self.logger.debug(f'{self.JOB_NAME}: Put {row_count} rows in the {table}.')
                if chunk_rows_already_existing:
                    existing_row_count += chunk_rows_already_existing
                    self.logger.debug(f'{self.JOB_NAME}: Existing {existing_row_count} rows already in the {table}.')
        except Exception as e:
            try:
                session.rollback()
            except Exception as e2:
                self.report_exception(e2, extra={
                    'msg': 'Failed to rollback the transaction',
                    'backup_key_name': self.backup_key_name})
            self.logger.error(f'Error when inserting row into {table}: {e}')
            raise
        else:
            if row_count:
                msg = (f'{self.JOB_NAME}: Finished putting {row_count} rows in the database.'
                       f'And there were {existing_row_count} existing rows that were not re-inserted.')
            else:
                msg = (f'{self.JOB_NAME}: Non New Records are added but a snapshot is added.'
                       f'And there were {existing_row_count} existing rows that were not re-inserted.')
            self.logger.info(msg)
            session.commit()

    def _handle_generic_exception(self, e, ping_slack):
        self.report_exception(e, extra={'backup_key_name': self.backup_key_name})
        if ping_slack:
            self.report_error_to_slack(e)

    def run(self, ping_slack=False, path=None, content=None, content_type=None,
            sheet_names=None, use_client=True, backup_data=True,
            ignore_missing_fields=True):
        """Entrypoint for any ETL job
        Argumements:
            ping_slack (bool): should alert slack on error
            path (str): file path of data
            content (?): source content
            content_type (str): input data type
            use_client (bool): use the given client for accessing data
            ignore_missing_fields (bool): drop columns that are not defined in the provided model mapping
                                      instead of raising an error.
        """
        try:
            with self.get_session() as session:
                data = self._extract(session, path=path, content=content, content_type=content_type,
                                     sheet_names=sheet_names, use_client=use_client, backup_data=backup_data)
                data_gen = self._transform(session, data)
                self._load(session, data_gen)
        except CastingError as e:
            self._handle_generic_exception(e, ping_slack)
            self.logger.exception(*e.get_logger_args(), extra=e.get_extra())
        except NothingToProcess:
            self.logger.info('There is nothing new to process.')
        except Exception as e:
            self._handle_generic_exception(e, ping_slack)
            self.logger.exception(str(e))

    def reload(self):
        """
        Reload from pickle dump of the last run of the client.
        Meant for local usage only.
        """
        data = self._load_state_after_client_response()
        try:
            with self.get_session() as session:
                data_gen = self._transform(session, data)
                self._load(session, data_gen)
        except CastingError as e:
            self._handle_generic_exception(e, ping_slack=False)
            self.logger.exception(*e.get_logger_args(), extra=e.get_extra())
        except Exception as e:
            self._handle_generic_exception(e, ping_slack=False)
