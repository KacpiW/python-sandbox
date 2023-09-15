import unittest
from unittest.mock import patch, MagicMock
from end_to_end.load import Loader, copy_object_between_buckets


class TestLoader(unittest.TestCase):
    @patch('mysql.connector.connect')
    def test_create_connection(self, mock_connect):
        mock_connect.return_value = MagicMock()
        loader = Loader(host='localhost', user='user',
                        passwd='passwd', db='db')
        mock_connect.assert_called_once_with(
            host='localhost', user='user', passwd='passwd', database='db')

    @patch('mysql.connector.connect')
    def test_insert_data(self, mock_connect):
        mock_cursor = MagicMock()
        mock_connect.return_value = MagicMock(
            cursor=MagicMock(return_value=mock_cursor))
        loader = Loader(host='localhost', user='user',
                        passwd='passwd', db='db')
        loader.insert_data('test_table', 'col1, col2', ('data1', 'data2'))
        mock_cursor.execute.assert_called_once()
        mock_connect.return_value.commit.assert_called_once()

    @patch('boto3.resource')
    def test_copy_object_between_buckets(self, mock_boto3):
        mock_boto3.return_value = MagicMock()
        copy_object_between_buckets(
            'extract_bucket', 'load_bucket', 'file_name')
        mock_boto3.assert_called_once_with('s3')
        mock_boto3.return_value.meta.client.copy.assert_called_once()


if __name__ == '__main__':
    unittest.main()
