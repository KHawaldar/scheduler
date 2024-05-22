import unittest
import uuid
from unittest.mock import patch

from service.reports.icbcmanager import ICBCManager


class TestICBCManager(unittest.TestCase):

    def setUp(self):
        self.kpiresponse = {
            'total_recall': '95',
            'auditor_fails_count': '200'
        }

    def test_when_total_recall_is_99_auditor_fails_count_200_then_model_performance_is_good(self):
        self.kpiresponse = {
            'icbc_calculation_kpis': {
                'total_recall': '99.5',
                'auditor_fails_count': '200'
            }
        }

        model_performance = ICBCManager(self.kpiresponse).get_model_performance(
            self.kpiresponse['icbc_calculation_kpis'])
        self.assertEqual(model_performance['set_pfc_in_silent_mode'], False,
                         "Expecting False, but got True")

    def test_when_total_recall_is_99_auditor_fails_count_100_then_model_performance_is_good(self):
        self.kpiresponse = {
            'icbc_calculation_kpis': {
                'total_recall': '99.5',
                'auditor_fails_count': '100'
            }
        }

        model_performance = ICBCManager(self.kpiresponse).get_model_performance(
            self.kpiresponse['icbc_calculation_kpis'])
        self.assertEqual(model_performance['set_pfc_in_silent_mode'], False,
                         "Expecting False, but got True")

    def test_when_total_recall_is_99_auditor_fails_count_99_then_model_performance_is_good(self):
        self.kpiresponse = {
            'icbc_calculation_kpis': {
                'total_recall': '99.5',
                'auditor_fails_count': '99'
            }
        }

        model_performance = ICBCManager(self.kpiresponse).get_model_performance(
            self.kpiresponse['icbc_calculation_kpis'])
        self.assertEqual(model_performance['set_pfc_in_silent_mode'], False,
                         "Expecting False, but got True")

    def test_when_total_recall_is_98_auditor_fails_count_99_then_model_performance_is_good(self):
        self.kpiresponse = {
            'icbc_calculation_kpis': {
                'total_recall': '98',
                'auditor_fails_count': '99'
            }
        }
        model_performance_impact = ICBCManager(self.kpiresponse).get_model_performance(
            self.kpiresponse['icbc_calculation_kpis'])
        self.assertEqual(model_performance_impact['set_pfc_in_silent_mode'],
                         False, "Expecting False, but got True")

    def test_when_total_recall_is_98_auditor_fails_count_299_then_model_performance_is_bad(self):
        self.kpiresponse = {
            'icbc_calculation_kpis': {
                'total_recall': 98,
                'auditor_fails_count': 299
            }
        }
        model_performance_impact = ICBCManager(self.kpiresponse).get_model_performance(
            self.kpiresponse['icbc_calculation_kpis'])
        self.assertEqual(model_performance_impact['set_pfc_in_silent_mode'],
                         True, "Expecting True, but got False")

    def test_when_total_recall_is_100_auditor_fails_count_299_then_model_performance_is_good(self):
        self.kpiresponse = {
            'icbc_calculation_kpis': {
                'total_recall': 100,
                'auditor_fails_count': '299'
            }
        }

        model_performance_impact = ICBCManager(self.kpiresponse).get_model_performance(
            self.kpiresponse['icbc_calculation_kpis'])
        self.assertEqual(model_performance_impact['set_pfc_in_silent_mode'],
                         False, "Expecting False, but got True")

    def test_when_total_recall_is_100_auditor_fails_count_99_then_model_performance_is_good(self):
        self.kpiresponse = {
            'icbc_calculation_kpis': {
                'total_recall': 100,
                'auditor_fails_count': '99'
            }
        }

        model_performance_impact = ICBCManager(self.kpiresponse).get_model_performance(
            self.kpiresponse['icbc_calculation_kpis'])
        self.assertEqual(model_performance_impact['set_pfc_in_silent_mode'], False,
                         "Expecting False, but got True")

    def test_when_no_total_recall_then_model_performance_should_raise_value_error(self):
        self.kpiresponse = {
            'icbc_calculation_kpis': {
                'auditor_fails_count': '99'
            }
        }

        with self.assertRaises(ValueError) as context:
            ICBCManager(self.kpiresponse).get_model_performance(self.kpiresponse['icbc_calculation_kpis'])
        self.assertEqual(str(context.exception), 'total recall or auditor_fails_count not calculated')

    def test_when_no_auditor_fails_count_then_model_performance_should_raise_value_error(self):
        self.kpiresponse = {
            'icbc_calculation_kpis': {
                'total_recall': '99'
            }
        }

        with self.assertRaises(ValueError) as context:
            ICBCManager(self.kpiresponse).get_model_performance(self.kpiresponse['icbc_calculation_kpis'])
        self.assertEqual(str(context.exception), 'total recall or auditor_fails_count not calculated')

    def test_when_total_recall_is_none_then_model_performance_should_raise_value_error(self):
        self.kpiresponse = {
            'icbc_calculation_kpis': {
                'total_recall': None,
                'auditor_fails_count': '99'
            }
        }
        with self.assertRaises(ValueError) as context:
            ICBCManager(self.kpiresponse).get_model_performance(self.kpiresponse['icbc_calculation_kpis'])
        self.assertEqual(str(context.exception), 'total recall or auditor_fails_count is None')

    def test_when_auditor_fails_count_is_none_then_model_performance_should_raise_value_error(self):
        self.kpiresponse = {
            'icbc_calculation_kpis': {
                'total_recall': '24',
                'auditor_fails_count': None
            }
        }
        with self.assertRaises(ValueError) as context:
            ICBCManager(self.kpiresponse).get_model_performance(self.kpiresponse['icbc_calculation_kpis'])
        self.assertEqual(str(context.exception), 'total recall or auditor_fails_count is None')

    @patch('service.reports.icbcmanager.SampleRate.sameplerate_upsert')
    def test_check_the_model_performance_and_actioned_icbc_service_when_model_performance_is_bad(
            self,
            samplerate_upsert_mock):
        kpiresponse = {
            'icbc_calculation_kpis': {
                'total_recall': '75',
                'auditor_fails_count': '500',
                'sampled_questions': '10000',
            }
        }
        result = {
            'is_value_inserted': True
        }
        samplerate_upsert_mock.return_value = result
        correlation_id: str = str(uuid.uuid4())
        response = (ICBCManager(kpiresponse).
                    check_the_model_performance_and_actioned_icbc_service(correlation_id=correlation_id))
        self.assertEqual(response['message'], "pfc is in silent mode", "pfc is not in silent mode")

    @patch('service.reports.icbcmanager.SampleRate.sameplerate_upsert')
    def test_check_the_model_performance_and_actioned_icbc_service_when_sample_rate_not_inserted_successfully(
            self,
            samplerate_upsert_mock):
        kpiresponse = {
            'icbc_calculation_kpis': {
                'total_recall': '75',
                'auditor_fails_count': '500',
                'sampled_questions': '10000',
            }
        }
        result = {
            'is_value_inserted': False,
            'key': 'sample_rate_v2_US2',
            'sample_rate': {
                'total_recall': '75',
                'auditor_fails_count': '500',
                'sampled_questions': '10000',
            }
        }

        samplerate_upsert_mock.return_value = result
        correlation_id: str = str(uuid.uuid4())
        response = (ICBCManager(kpiresponse).
                    check_the_model_performance_and_actioned_icbc_service(correlation_id=correlation_id))
        sample_rate: dict = result['sample_rate']
        key: str = result['key']
        expected_result: str = f'Sample rate is not inserted properly {sample_rate} for the key {key}'
        self.assertEqual(response['error'], expected_result, "sample rate is inserted successfully")

    def test_check_the_model_performance_and_actioned_icbc_service_when_get_model_performance_throws_error(
            self):
        kpiresponse: dict = {
            'icbc_calculation_kpis': {
                'total_recall': None,
                'auditor_fails_count': '500',
                'sampled_questions': '10000'
            }

        }
        correlation_id: str = str(uuid.uuid4())
        response = (ICBCManager(kpiresponse).
                    check_the_model_performance_and_actioned_icbc_service(correlation_id=correlation_id))
        self.assertIsInstance(response['error'], ValueError)
        self.assertEqual(str(response['error']), 'total recall or auditor_fails_count is None')

    def test_check_the_model_performance_and_actioned_icbc_service_when_icbc_calculation_kpis_is_null(self):
        kpiresponse: dict = {}
        correlation_id: str = str(uuid.uuid4())
        response = (ICBCManager(kpiresponse).
                    check_the_model_performance_and_actioned_icbc_service(correlation_id=correlation_id))
        self.assertEqual(response['error'], "icbc_calculation_kpis is null", "icbc_calculation_kpis is not null")

    def test_check_the_model_performance_hen_overall_performance_is_good_then_check_at_entity_level(self):
        response: dict = {
            'icbc_calculation_kpis': {
                "total_recall": "0", "local_recall": "0", "total_bypass": "0.0", "local_bypass": "0",
                "auditor_fails_count": "0"},

            'status': 'danger',
            'kibana_kpis': {
                "total recall: 0% || total bypass: 0.0% || local recall: 0% || local bypass: 0%"
            }

        }
        performance_message: dict = ICBCManager(response).check_the_model_performance_and_actioned_icbc_service()
        self.assertEqual(performance_message['message'], 'company recall',
                         "company is not recalled")

    def test_check_the_model_performance_when_overall_performance_is_good(self):
        response = {
            'icbc_calculation_kpis': {
                'total_recall': '99.56',
                'local_recall': '20.2',
                'total_bypass': '15.3',
                'local_bypass': '20.4',
                'auditor_fails_count': '45.0'
            },
            'status': 'danger',
            'kibana_kpis': {
                'Total Recall: 99.56% || Total Bypass: 15% || Local Recall: 20.2% || Local Bypass: 20.4%'
            }
        }

        response: dict = ICBCManager(response).check_the_model_performance_and_actioned_icbc_service()
        self.assertEqual(response.get('message'), 'company recall',
                         'company recall is not called')


if __name__ == '__main__':
    unittest.main()