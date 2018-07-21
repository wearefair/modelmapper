import boto3
import mmh3
import gzip
import datetime
import logging

from botocore.client import Config
from sqlalchemy import exc

from modelmapper.cleaner import Cleaner
from modelmapper.slack import slack
from modelmapper.misc import generator_chunker, generator_updater


class Fetcher(Cleaner):
    """
    Subclass this for your data processing and define the BUCKET_NAME, RAW_KEY_MODEL and RECORDS_MODEL
    """

    BUCKET_NAME = None
    RAW_KEY_MODEL = None
    RECORDS_MODEL = None
    S3KEY_DATETIME_FORMAT = '%Y/%m/autoims_raw_%Y_%m_%d__%H_%M_%S.gzip'
    SQL_CHUNK_ROWS = 300
    logger = logging.getLogger(__name__)

    def __init__(self, *args, **kwargs):
        self.JOB_NAME = self.__class__.__name__
        self.DUMP_FILEPATH = f'/tmp/{self.JOB_NAME}_dump'
        self.all_rows_signatures = set()
        super().__init__(*args, **kwargs)

    def get_client_data(self):
        """
        The client should return data that the importer will clean and import.
        """
        raise NotImplementedError('Please implement the get_client_data in your subclass.')

    def sentry_error(self, e):
        raise NotImplementedError('Please implement this method in your subclass.')

    def get_hash_of_bytes(self, item):
        return mmh3.hash(item)

    def get_hash_of_row(self, row):
        if isinstance(row, tuple):
            items = row
        elif isinstance(row, dict):
            items = row.items()
        row_bytes = b','.join([str(v).encode('utf-8') for k, v in items if k not in self.settings.ignore_fields_in_signature_calculation])  # NOQA
        return self.get_hash_of_bytes(row_bytes)

    def _verify_access_to_s3_bucket(bucket=BUCKET_NAME):

        config = Config(connect_timeout=.5, retries={'max_attempts': 1})

        s3_client = boto3.client('s3', config=config)
        s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)

    def get_file_from_s3(self, s3key):
        body = None
        s3_client = boto3.client('s3')
        s3fileobj = s3_client.get_object(Bucket=self.BUCKET_NAME, Key=s3key)
        if 'Body' in s3fileobj:
            body = s3fileobj['Body'].read()
            signature = self.get_hash_of_bytes(body)
            body = body.decode('utf-8')
        return body, signature

    def report_error_to_slack(self, e, msg='{} The {} failed: {}'):
        slack_handle_to_ping = f'<{self.settings.slack_handle_to_ping}>' if self.settings.slack_handle_to_ping else''
        try:
            msg = msg.format(slack_handle_to_ping, self.JOB_NAME, e)
        except Exception as e:
            pass
        self.slack()

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

    def _put_file_on_s3(self, content, s3key, metadata=None):
        s3_client = boto3.client('s3')
        self.logger.info('Putting {} on s3.'.format(s3key))
        s3_client.put_object(ACL='bucket-owner-full-control', Bucket=self.BUCKET_NAME, Key=s3key,
                             Metadata=metadata,
                             Body=content)

    def _backup_data_and_get_raw_key(self, session, data_raw_bytes):
        s3key = datetime.datetime.strftime(datetime.datetime.utcnow(), self.S3KEY_DATETIME_FORMAT)
        signature = mmh3.hash(data_raw_bytes)
        raw_key_id = self._create_raw_key(s3key, signature)
        data_to_dump = {"data_raw_bytes": data_raw_bytes, "raw_key_id": raw_key_id}
        data_json = json.dumps(data_to_dump, indent=2).encode('utf-8')
        with open(f'{self.DUMP_FILEPATH}.json', 'wb') as dump_file:
            dump_file.write(data_json)
        data_compressed = self._compress(data_raw_bytes)
        self.logger.info(f'Backing up the data into s3 bucket: {s3key}')
        metadata = {'compression': 'gzip'}
        self._put_file_on_s3(content=data_compressed, s3key=s3key, metadata=metadata)
        return raw_key_id

    def _create_raw_key(self, session, s3key, signature):
        try:
            raw_key = self.RAW_KEY_MODEL(s3key=s3key, signature=signature)
            session.add(raw_key)
            session.commit()
        except exc.IntegrityError as e:
            session.rollback()
            raise
        return raw_key.id

    def omit_unchanged_data(self, chunk):
        for row in chunk:
            row['signature'] = signature = self.get_row_signature(row)
            if signature not in self.all_rows_signatures:
                self.all_rows_signatures.add(signature)
                yield row

    def get_row_signature(self, row):
        sorted_row = sorted(row.items(), key=lambda t: t[0])
        return self.get_hash_of_row(sorted_row)

    def get_session(self):
        raise NotImplementedError('Please provide a function for SQLAlchemy session.')

    def insert_row_data_to_db(self, session, data_gen, row_metadata):
        self.logger.info(f"{self.JOB_NAME}: Inserting data into db")
        table = self.__table__

        row_count = previous_row_count = 0

        self.all_rows_signatures = set()

        data_gen = generator_updater(data_gen, **row_metadata)
        chunks = generator_chunker(data_gen, chunk_size=self.SQL_CHUNK_ROWS)
        try:
            for chunk in chunks:
                new_chunk = list(self.omit_unchanged_data(chunk)) if self.settings.ignore_duplicate_rows_when_importing else list(chunk)  # NOQA
                row_count += len(new_chunk)
                if row_count - previous_row_count > 100:
                    previous_row_count = row_count
                    self.logger.debug(f'{self.JOB_NAME}: Putting row {row_count} in the database.')

                session.execute(table.insert(), new_chunk)
        except Exception as e:
            self.logger.exception(f'Error when inserting row into {table}: {e}')
            try:
                session.rollback()
            except Exception as e2:
                self.logger.error('Failed to rollback the transaction: {}'.format(e2))
        else:
            session.commit()

    def do_fetch(self, session, ping_slack=False, path=None, content=None, content_type=None,
                 sheet_names=None, use_client=True, backup_data=True):

        invalid_choices = [
            (not (path and content and use_client),
             ValueError('If path and content are None, the client must be used.')),
            ((path or content) and use_client,
             ValueError('If path or content are defined, the client should not be used.')),
            (not use_client and backup_data,
             ValueError('Data can be only backed up if the client is used.'))
        ]

        for case, err in invalid_choices:
            if case:
                raise err

        if backup_data:
            self._verify_access_to_s3_bucket()

        self.logger.info(f'Starting the {self.JOB_NAME} ...')

        if use_client:
            content = self.get_client_data()

        if backup_data:
            content = content.encode('utf-8') if isinstance(content, str) else content
            raw_key_id = self._backup_data_and_get_raw_key(session, data_raw_bytes=content)
        else:
            raw_key_id = None

        cleaned_data_gen = self.clean(content_type=content_type, path=path,
                                      content=content, sheet_names=sheet_names)

        row_metadata = {'raw_key_id': raw_key_id}
        self.insert_row_data_to_db(self, session, data_gen=cleaned_data_gen, row_metadata=row_metadata)

    def fetch(self, ping_slack=False, path=None, content=None, content_type=None,
              sheet_names=None, use_client=True, backup_data=True):
        try:
            with self.get_session() as session:
                self.do_fetch(session, ping_slack=False, path=None, content=None, content_type=None,
                              sheet_names=None, use_client=True, backup_data=True)
        except Exception as e:
            self.sentry_error(e)
            self.logger.exception(str(e))
            if ping_slack:
                self.report_error_to_slack(e)
