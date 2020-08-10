import os
import paramiko
from contextlib import contextmanager
from modelmapper.exceptions import NothingToProcess


class SftpClient():

    def __init__(self, host, port, user, password, timeout=30):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout

    @contextmanager
    def get_connection(self, conn=None):
        if conn:
            yield conn
        else:
            transport = paramiko.Transport((self.host, self.port))
            transport.connect(username=self.user, password=self.password)
            try:
                conn = paramiko.SFTPClient.from_transport(transport)
                conn.get_channel().settimeout(self.timeout)
                yield conn
            finally:
                transport.close()


class SftpClientMixin:

    SFTP_HOST = None
    SFTP_PORT = None
    SFTP_USER = None
    SFTP_PASSWORD = None
    SFTP_READ_FOLDER = None
    SFTP_WRITE_FOLDER = None
    SFTP_TIMEOUT = 30  # sec

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._files_downloaded_from_sftp = set()
        self.sftp_client = SftpClient(
            host=self.SFTP_HOST, port=self.SFTP_PORT,
            user=self.SFTP_USER, password=self.SFTP_PASSWORD,
            timeout=self.SFTP_TIMEOUT)

    def _sftp_download_reporter(self, current, total):
        self.logger.info(f"sftp: {current} bytes uploaded out of {total}")

    def get_list_of_files_on_sftp_gen(self, path=None, conn=None):
        path = path if path else self.SFTP_READ_FOLDER
        with self.sftp_client.get_connection(conn=conn) as conn:
            keys = conn.listdir_iter(path)
            for key in keys:
                yield os.path.join(path, key)

    def get_file_content_from_sftp(self, key=None, conn=None):
        contents = None
        localpath = f"/tmp/{hash(key)}"
        self.logger.info(f"Sftp: Downloading {key}")
        with self.sftp_client.get_connection(conn=conn) as conn:
            conn.get(localpath=localpath, remotepath=key,
                     callback=self._sftp_download_reporter)
        with open(localpath, 'rb') as the_file:
            contents = the_file.read()
        return contents

    def get_files_from_sftp_gen(self, path=None, conn=None):
        with self.sftp_client.get_connection(conn=conn) as conn:
            keys_gen = self.get_list_of_files_on_sftp_gen()
            self._files_downloaded_from_sftp = set()
            for key in keys_gen:
                self._files_downloaded_from_sftp.add(key)
                content = self.get_file_content_from_sftp(path=None, conn=None)
                yield content, key

    def post_pickup_cleanup(self, conn=None):
        if self.settings.delete_source_object_after_backup:
            with self.sftp_client.get_connection(conn=conn) as conn:
                for key in self._files_downloaded_from_sftp:
                    result = conn.remove(key)
                    if result != paramiko.SFTP_OK:
                        self.logger.error(f"Unable to delete the {key}. Got error code: {result}")

    def get_client_data(self):
        try:
            content, key = next(self.get_files_from_sftp_gen(path=self.SFTP_READ_FOLDER))
        except StopIteration:
            raise NothingToProcess('No files to process') from None
        return content, key
