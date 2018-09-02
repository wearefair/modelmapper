from contextlib import contextmanager
from functools import partial
from socket import error as SocketError
import logging

import paramiko

from misc import cached_property


class ClientException(Exception):
    pass


class ClientSSHException(ClientException):
    pass


class BaseClient(object):
    def __init__(self):
        self.logger = logging

    @cached_property
    def seen_keys(self, session, raw_key_model):
        return set(map(lambda x: x[0], session.query(raw_key_model.key)))

    def extract(self):
        raise NotImplemented('Implement extract in your implementation')


class SFTPClient(BaseClient):
    def __init__(self, *args, settings=None, **kwargs):
        super(*args, **kwargs).__init__()
        self.settings = settings
        if not settings:
            self.settings.update(kwargs)

    @contextmanager
    @staticmethod
    def get_sftp(self, hostname, **auth_kwargs):
        """Gracefully opens and closes Paramiko SFTPClient instance.

        Args:
            hostname (str): SFTP hostname you want to connect to.
            **auth_kwargs (type): .

        Raises:             paramiko.SSHException: Any error establishing an SSH Session.
                            paramiko.BadHostKeyException: Server hostkey could not be verified.
                            paramiko.AuthenticationException: Auth failed.
                            SocketError (socker.error): Low-level socket error. Accompanied by sys (errno, string).
        Example:
            with self.get_sftp() as sftp:
                # use sftp client
                sftp.get(remotepath, localpath)

        See Also:
            http://docs.paramiko.org/en/2.4/api/client.html
            http://docs.paramiko.org/en/2.4/api/sftp.html
        """
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

    @classmethod
    def extract(cls, remote_dirpath, *args, **kwargs):
        """Exposed function.

        Args:
            cls (type): Class constructor
            remote_dirpath (str): Description of parameter `remote_dirpath`.

        Returns:
            type: Description of returned object.

        Raises:            ExceptionName: Why the exception is raised.

        """
        sftp_client = cls(*args, **kwargs)
        return sftp_client.extract(remote_dirpath)

    def _extract(self, remote_dirpath, localpath, historical=True):
        get_fn = self.get if isinstance(localpath, str) else self.getfo
        diff = self.contents.difference(self.seen_keys)

        if len(diff) > 0:
            new_file = sorted(diff)[0]
            self.logger.info('Files not already extracted: {}'.format(diff))

        return get_fn('{}/{}'.format(remote_dirpath, new_file)), new_file

    @cached_property
    def contents(self, remotepath):
        """Lists contents of SFTP at given path.

        Args:
            remotepath (str): Location on FTP server.

        Returns:
            list[str]: SFTP directory contents.
        """
        with self.get_sftp() as sftp:
            return sftp.listdir(remotepath)

    def getfo(self, remotepath, file_like_obj, callback=None):
        """Wrapper around Paramiko sftp.get().

        Args:
            remotepath (type): Description of parameter `remotepath`.
            file_like_obj (type): Description of parameter `file_like_obj`.
            callback (type): Description of parameter `callback`.

        Returns:
            type: Description of returned object.

        Raises:            ExceptionName: Why the exception is raised.

        """
        callback = callback or self.default_callback

        with self.get_sftp() as sftp:
            self.logger.info('Extracting {}'.format(remotepath))

            bytes_read = sftp.get(remotepath, file_like_obj, callback=callback)
            if bytes_read < 1:
                self.logger.info('SFTP did not transfer any data')
            return bytes

    def get(self, remotepath, localpath, callback=None):
        callback = callback or self.default_callback

        with self.get_sftp() as sftp:
            bytes_read = sftp.get(remotepath, localpath, callback=callback)
            if bytes_read < 1:
                self.logger.info('SFTP did not transfer any data')

    def default_callback(self):
        def cb(logger, seen, total):
            logger.info('Read {} bytes of total {} bytes.'.format(seen, total))
        return partial(cb, logger=self.logger)
