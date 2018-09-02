from contextlib import contextmanager
from io import BytesIO
from functools import partial
from socket import error as SocketError
import logging

import paramiko


class ClientException(Exception):
    pass


class ClientSSHException(ClientException):
    pass


class BaseClient(object):
    def __init__(self):
        self.logger = logging

    def seen_keys(self, session, raw_key_model):
        return set(map(lambda x: x[0], session.query(raw_key_model.key)))

    def extract(self):
        pass


class SFTPClient(BaseClient):
    def __init__(self, *args, settings=None, **kwargs):
        super(*args, **kwargs).__init__()
        self.settings = {}
        if not settings:
            self.settings.update(kwargs)

    @contextmanager
    def get_sftp(self, hostname, **auth_kwargs):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # attempts connection
        try:
            ssh.connect(hostname, **auth_kwargs)
            self.logger.info('Connecting to {}'.format(hostname))
            yield ssh.open_sftp()
        except (paramiko.SSHException, paramiko.BadHostKeyException,
                paramiko.AuthenticationException, SocketError) as e:
                raise ClientSSHException('SFTP CLient {}'.format(e)) from None
        finally:
            ssh.close()

    def getfo(self, remotepath, callback=None):
        callback = callback or self.default_callback

        with self.get_sftp() as sftp:
            file_like_obj = BytesIO()
            self.logger.info('Extracting {}'.format(remotepath))

            bytes_read = sftp.get(remotepath, file_like_obj, callback=callback)
            if bytes_read < 1:
                self.logger.info('SFTP did not transfer any data')

    def get(self, remotepath, localpath):
        with self.get_sftp() as sftp:
            sftp = None
            pass

    def default_callback(self):
        def cb(logger, seen, total):
            logger.info('Read {} bytes of total {} bytes.'.format(seen, total))
        return partial(cb, logger=self.logger)
