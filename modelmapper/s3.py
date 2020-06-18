from modelmapper.misc import cached_property
from modelmapper.exceptions import NothingToProcess

try:
    import boto3
    from botocore.client import Config
except ImportError:
    class boto3:
        def client(self, *args, **kwargs):
            raise ImportError('Please install Boto3')

    def Config(*args, **kwargs):
        raise NotImplementedError('Please install Boto3')


class S3AccessError(ValueError):
    pass


class S3Base:

    def _verify_access_to_s3_bucket(self, bucket):
        try:
            self.s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        except Exception as e:
            raise S3AccessError(f'Error when trying to access {bucket}: {e}')

    @cached_property
    def s3_client(self):
        config = Config(connect_timeout=.5, retries={'max_attempts': 1})
        return boto3.client('s3', config=config)

    def get_list_of_files_on_s3(self, bucket, prefix='', max_keys=1000):
        s3_list_response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)
        return {i['Key'] for i in s3_list_response.get('Contents', [])}

    def get_file_from_s3(self, bucket, s3key):
        s3fileobj = self.s3_client.get_object(Bucket=bucket, Key=s3key)
        if 'Body' in s3fileobj:
            return s3fileobj['Body'].read()

    def get_files_from_s3_gen(self, bucket=None, prefix='', max_keys=1000):
        keys = self.get_list_of_files_on_s3(bucket=bucket, prefix=prefix, max_keys=max_keys)
        self._files_downloaded_from_s3 = set()
        for s3key in keys:
            self._files_downloaded_from_s3.add(s3key)
            content = self.get_file_from_s3(bucket=bucket, s3key=s3key)
            yield content, s3key

    def put_file_on_s3(self, content, key, metadata=None):
        self.logger.info('Putting {} on s3.'.format(key))
        self.s3_client.put_object(ACL='bucket-owner-full-control', Bucket=self.BUCKET_NAME, Key=key,
                                  Metadata=metadata,
                                  Body=content)


class S3Mixin(S3Base):

    BUCKET_NAME = None

    def verify_access_to_backup_source(self):
        self._verify_access_to_s3_bucket(bucket=self.BUCKET_NAME)


class S3ClientMixin(S3Base):

    SOURCE_BUCKET_NAME = None

    def verify_access_to_backup_source(self):
        self._verify_access_to_s3_bucket(bucket=self.SOURCE_BUCKET_NAME)

    def post_packup_cleanup(self):
        objects = [{'Key': i} for i in self._files_downloaded_from_s3]
        self.s3_client.delete_objects(
            Bucket=self.SOURCE_BUCKET_NAME,
            Delete={
                'Objects': objects,
                'Quiet': False
            },
        )

    def get_client_data(self):
        try:
            content, key = next(self.get_files_from_s3_gen(bucket=self.SOURCE_BUCKET_NAME, max_keys=1))
        except StopIteration:
            raise NothingToProcess('No files to process') from None
        return content, key
