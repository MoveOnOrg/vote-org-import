from datetime import datetime

import pytest
from _pytest.monkeypatch import MonkeyPatch

from import_new_files import import_new_files


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)

class Test():
    monkeypatch = MonkeyPatch()
    file_list = []

    # Mock what database would return for last_run.
    def mock_get_psql_results(self, args):
        return [{'last_run': datetime.strptime('2020-03-01 12:00:00', '%Y-%m-%d %H:%M:%S')}]

    # Mock what S3 would return for file list.
    def mock_s3_file_list(self, args):
        return self.file_list

    def test_import_new_files(self):
        Test.monkeypatch.setattr("import_new_files.get_psql_results", self.mock_get_psql_results)
        Test.monkeypatch.setattr("import_new_files.s3_file_list", self.mock_s3_file_list)
        # All args are mocked, but still required.
        args = {
            'AWS_ACCESS_KEY': 'mock',
            'AWS_SECRET_KEY': 'mock',
            'S3_BUCKET': 'mock',
            'S3_BUCKET_REGION': 'mock',
            'DB_HOST': 'mock',
            'DB_PORT': 'mock',
            'DB_USER': 'mock',
            'DB_PASS': 'mock',
            'DB_NAME': 'mock',
            'DB_TABLE': 'mock',
            'LAST_RUN_SCRIPT': '2020-02-01-01 00:00:00'
        }
        args = Struct(**args)
        # Nothing new to import.
        self.file_list = ['2020-01-01-12-30-00/hash.csv']
        result = import_new_files(args)
        assert result == {'imports': 0, 'last': '2020-03-01 12:00:00'}
        # Something new to import.
        self.file_list = ['2020-04-01-12-30-00/hash.csv']
        result = import_new_files(args)
        assert result == {'imports': 1, 'last': '2020-04-01 12:30:00'}
