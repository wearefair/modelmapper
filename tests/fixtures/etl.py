from modelmapper import ETL


class BasicETL(ETL):

    def get_client_data(self):
        pass

    def report_exception(self, e):
        pass

    def encrypt_data(self, gen):
        pass

    def backup_data(self, content, key, metadata):
        pass

    def get_session(self):
        pass

    def insert_chunk_of_data_to_db(self, session, table, chunk):
        pass

    def encrypt_row_fields(self, cleaned_data_gen):
        pass

    def verify_access_to_backup_source(self):
        pass

    def pre_clean_transform(self, session, data_gen):
        pass

    def post_clean_transform(self, session, data_gen):
        pass
