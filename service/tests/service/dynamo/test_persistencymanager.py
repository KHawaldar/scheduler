import unittest
import uuid
from unittest.mock import MagicMock, patch
from service.dynamo.persistencymanager import PersistencyManager


class TestPersistencyManager(unittest.TestCase):

    def setUp(self):
        self.persistency_manager = PersistencyManager("dummy_table")

    def test_persistency_manager_is_fail_to_intialized(self):
        try:
            PersistencyManager("")
        except ValueError:
            self.assertRaises(ValueError)

    @patch('service.dynamo.persistencymanager.get_dynamo')
    def test_get_the_value(self, dynamo_mock):
        mock = MagicMock()
        mock.get.return_value = "some response"
        dynamo_mock.return_value = mock
        self.assertEqual(self.persistency_manager.get_the_value("dummy_key"), "some response")
        dynamo_mock.assert_called_once()

    @patch('service.dynamo.persistencymanager.get_dynamo', side_effect=Exception)
    def test_get_the_value_throws_excecption(self, dynamo_mock):
        # when
        result = self.persistency_manager.get_the_value("dummy_key")
        # then
        self.assertRaises(Exception)
        self.assertEqual(result, None, "result is not None")

    @patch('service.dynamo.persistencymanager.get_dynamo')
    def test_upsert(self, dynamo_mock):
        value = {
            "key1": "value1"
        }
        mock = MagicMock()
        mock.upsert.return_value = "some response"
        dynamo_mock.return_value = mock
        logging_msg: dict = {
            'success_message': 'success'
        }
        correlation_id: str = str(uuid.uuid4())
        # when
        result = self.persistency_manager.upsert_value("key", value, logging_msg, correlation_id, 0)
        # then
        self.assertEqual(result, "some response", "result is not same")

        dynamo_mock.assert_called_once_with("dummy_table")
        mock_upsert = dynamo_mock.return_value.upsert
        mock_upsert.assert_called_once_with(
            key="key", value=value,
            correlation_id=correlation_id,
            ttl=0
        )

    @patch('service.dynamo.persistencymanager.get_dynamo')
    # whenever upsert method throws exception, it sends the result as False
    def test_upsert_throws_error(self, dynamo_mock):
        value = {
            "key1": "value1"
        }
        mock = MagicMock()
        mock.upsert.return_value = False
        dynamo_mock.return_value = mock
        logging_msg: dict = {
            'success_message': 'success'
        }
        result = self.persistency_manager.upsert_value("key", value, logging_msg, str(uuid.uuid4()), 0)
        self.assertEqual(result, False, "Exception not thrown")

    def test_persistency_manager_for_initialization_error(self):
        try:
            PersistencyManager("    ")
        except ValueError:
            self.assertRaises(ValueError)


if __name__ == '__main__':
    unittest.main()