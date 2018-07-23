try:
    import boto3
    from botocore.client import Config
except ImportError:
    class boto3:
        def client(self, *args, **kwargs):
            raise ImportError('Please install Boto3')

    def Config(*args, **kwargs):
        raise NotImplementedError('Please install Boto3')


class S3Mixin:

    def verify_access_to_backup_source(self):
        self._verify_access_to_s3_bucket()

    def _verify_access_to_s3_bucket(self):

        config = Config(connect_timeout=.5, retries={'max_attempts': 1})

        s3_client = boto3.client('s3', config=config)
        s3_client.list_objects_v2(Bucket=self.BUCKET_NAME, MaxKeys=1)

    def get_file_from_s3(self, s3key):
        body = None
        s3_client = boto3.client('s3')
        s3fileobj = s3_client.get_object(Bucket=self.BUCKET_NAME, Key=s3key)
        if 'Body' in s3fileobj:
            body = s3fileobj['Body'].read()
            signature = self.get_hash_of_bytes(body)
            body = body.decode('utf-8')
        return body, signature

    def put_file_on_s3(self, content, key, metadata=None):
        s3_client = boto3.client('s3')
        self.logger.info('Putting {} on s3.'.format(key))
        s3_client.put_object(ACL='bucket-owner-full-control', Bucket=self.BUCKET_NAME, Key=key,
                             Metadata=metadata,
                             Body=content)
