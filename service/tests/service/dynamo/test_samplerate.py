import unittest
import uuid
from unittest.mock import MagicMock, patch
from service.configs import config
from service.dynamo.samplerate import SampleRate


class TestSampleRate(unittest.TestCase):

    @patch('service.dynamo.samplerate.PersistencyManager.get_persistency_manager')
    def test_get_sample_rate_happy_path(
            self,
            persistency_manager_mock):
        mock_response: MagicMock = MagicMock()
        sample_rate_result = {
            '100': {
                'date_added': '2024-01-01T12:04:34',
                'total_recall': 75,
                'sampled_questions': 56890,
                'auditor_fails_count': 390
            }
        }
        mock_response.get_the_value.return_value = sample_rate_result
        persistency_manager_mock.return_value = mock_response
        correlation_id: str = str(uuid.uuid4())
        key: str = "sample_rate_v2_US2"

        sample_rate_result: dict = SampleRate(config.icbc_params_table_name).get_sample_rate(
            correlation_id=correlation_id, key=key)
        self.assertIsNotNone(sample_rate_result, "sample rate is null")

    @patch('service.dynamo.samplerate.PersistencyManager.get_persistency_manager')
    def test_sameplerate_upsert_when_sample_rate_key_is_not_found(
            self,
            persistency_manager_mock

    ):
        mock_response: MagicMock = MagicMock()
        sample_rate_result = None
        mock_response.get_the_value.return_value = sample_rate_result
        mock_response.upsert_value.return_value = True
        persistency_manager_mock.return_value = mock_response

        response = {
            'total_recall': 56,
            'sampled_questions': 679,
            'auditor_fails_count': 450
        }
        correlation_id: str = str(uuid.uuid4())
        result: dict = SampleRate(config.icbc_params_table_name).sameplerate_upsert(response=response,
                                                                                    correlation_id=correlation_id)
        sample_rate_100_key: dict = result['sample_rate']

        sample_rate = sample_rate_100_key['100']
        is_value_inserted: str = result['is_value_inserted']
        self.assertIsNotNone(result, "result is null")
        self.assertEqual(sample_rate['total_recall'], response['total_recall'], "total recall is not equal")
        self.assertEqual(sample_rate['sampled_questions'], response['sampled_questions'],
                         "sampled questions are not equal")
        self.assertEqual(sample_rate['auditor_fails_count'], response['auditor_fails_count'],
                         "auditor fail is not equal")
        self.assertEqual(is_value_inserted, True, "Value is not inserted")

    @patch('service.dynamo.samplerate.PersistencyManager.get_persistency_manager')
    def test_sameplerate_upsert_when_sample_rate_key_is_found(
            self,
            persistency_manager_mock):
        mock_response: MagicMock = MagicMock()
        sample_rate_result = {
            '100': {
                'date_added': '2024:01:01T12:01:12',
                'total_recall': 45,
                'sampled_questions': 56908,
                'auditor_fails_count': 980
            }
        }
        mock_response.get_the_value.return_value = sample_rate_result

        persistency_manager_mock.return_value = mock_response

        response = {
            'total_recall': 56,
            'sampled_questions': 679,
            'auditor_fails_count': 450
        }
        correlation_id: str = str(uuid.uuid4())
        samplerate: SampleRate = SampleRate(config.icbc_params_table_name)
        samplerate.persistency_manager = MagicMock()
        result: dict = samplerate.sameplerate_upsert(response=response,
                                                     correlation_id=correlation_id)

        self.assertIsNotNone(result, "result is null")
        samplerate.persistency_manager.upsert.assert_not_called()


if __name__ == '__main__':
    unittest.main()