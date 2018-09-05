from contextlib import contextmanager
import logging
from socket import error as SocketError

import paramiko

from modelmapper.misc import cached_property


class ClientException(Exception):
    pass


class ClientSSHException(ClientException):
    pass


class BaseClient:
    def __init__(self, session, raw_key_model, *args, **kwargs):
        self.logger = kwargs.get('logger', logging.Logger(__name__))
        self.session = session
        self.raw_key_model = raw_key_model

    @cached_property
    def seen_keys(self, session, raw_key_model):
        return set(map(lambda x: x[0], session.query(raw_key_model.key)))

    def extract(self, *args, **kwargs):
        raise NotImplemented('Implement extract in your subclass')


class SFTPClient(BaseClient):
    def __init__(self, *args, **kwargs):
        try:
            model = kwargs.pop('raw_key_model')
            session = kwargs.pop('session')
        except KeyError:
            raise ClientSSHException() from None
        super(*args, session, model, **kwargs).__init__()
        self.auth = {
            'hostname': 'localhost',
            'username': '',
            'password': '',
        }
        self.settings.update(kwargs)

    @staticmethod
    @contextmanager
    def get_sftp(self, hostname, **auth_kwargs):
        """Gracefully opens and closes Paramiko SFTPClient instance.

        Args:
            hostname (str): SFTP hostname you want to connect to.
            **auth_kwargs (type): SSH/SFTP authentication parameters.

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
            remote_dirpath (str): Base directory where files are pulled.

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
        """Wrapper around Paramiko.SFTPClient.getfo().

        Args:
            remotepath (str): Full path on FTP to get
            file_like_obj (One of (BytesIO, StringIO)): Where to write raw bytes.
            callback (fn): Custom function

        Returns:
            file_like_obj (One of (BytesIO, StringIO)): Where to write raw bytes.
        """
        callback = callback or self.default_callback

        with self.get_sftp() as sftp:

            self.logger.info('Extracting {}'.format(remotepath))
            bytes_read = sftp.get(remotepath, file_like_obj, callback=callback)

            if not bytes_read:
                self.logger.info('SFTP did not transfer any data')

            return file_like_obj

    def get(self, remotepath, localpath, callback=None):
        """Wrapper around Paramiko.SFTPClient.get()."""
        callback = callback or self.default_callback

        with self.get_sftp() as sftp:
            bytes_read = sftp.get(remotepath, localpath, callback=callback)
            if not bytes_read:
                self.logger.info('SFTP did not transfer any data')

    def default_callback(self):
        """Default callback for all our wrapped Paramiko calls.
        Possibly dangerous because it relies on the underlying logger
        to have a info functiond defined.
        """
        def cb(seen, total):
            self.logger.info('Read {} bytes of total {} bytes.'.format(seen, total))
        return cb
