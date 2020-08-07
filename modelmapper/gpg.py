try:
    import gnupg
except ImportError:
    gnupg = None


class GPGFail(ValueError):
    pass


class GPGMixin:
    GPG_PUBLIC_KEY = ''
    GPG_PRIVATE_KEY = ''
    GPG_RECIPIENT = ''
    GPG_PASSPHRASE = ''

    def __init__(self, *args, **kwargs):
        if gnupg is None:
            raise ImportError('python-gnupg package needs to be installed.')
        super().__init__(*args, **kwargs)
        self.gpg = gnupg.GPG()
        self._gpg_import_keys()

    def _gpg_import_keys(self):
        for key in (self.GPG_PUBLIC_KEY, self.GPG_PRIVATE_KEY):
            if key:
                result = self.gpg.import_keys(key)
                if 'fingerprint' not in result.results[0]:
                    raise GPGFail(f'Unable to import the key: {result.results[0]}')
                else:
                    self.gpg.trust_keys(result.results[0]['fingerprint'], 'TRUST_ULTIMATE')

    def gpg_encrypt_file(self, input_file, output_file, recipient=None):
        recipient = recipient if recipient else self.GPG_RECIPIENT

        with open(input_file, 'rb') as f:
            status = self.gpg.encrypt_file(
                file=f,
                recipients=[recipient],
                output=output_file,
                always_trust=True,
            )

        self._log_errors('gpg_encrypt_file', status)

    def gpg_encrypt_content(self, content, recipient=None):
        """
        It is recommended for the content to be bytes.
        """
        recipient = recipient if recipient else self.GPG_RECIPIENT
        encrypted_data = self.gpg.encrypt(content, recipient)
        self._log_errors('gpg_encrypt_content', encrypted_data)

        return encrypted_data.data

    def gpg_decrypt_content(self, content, passphrase=None):
        """
        Returns bytes
        """
        passphrase = passphrase if passphrase else self.GPG_PASSPHRASE
        decrypted_data = self.gpg.decrypt(content, passphrase=passphrase, always_trust=True)
        self._log_errors('gpg_decrypt_content', decrypted_data)

        return decrypted_data.data

    def _log_errors(self, func_name, obj):
        if not obj.ok:
            msg = f'Failed to {func_name}: status: {obj.status} stderr: {obj.stderr}'
            self.logger.error(msg)
            raise GPGFail(msg)
