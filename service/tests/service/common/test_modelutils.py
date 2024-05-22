import unittest
import uuid
from datetime import datetime
from unittest.mock import MagicMock, patch

from service.common.dateutils import DateUtils
from service.common.modelutils import ModelUtils
from service.configs import config
from service.dynamo.persistencymanager import PersistencyManager
from service.enums.timetoliveenum import TimeToLive


class TestModelUtils(unittest.TestCase):
    def test_get_model_version_with_environment_suffix(self):
        model_version_with_env_suffix: str = ModelUtils.get_model_version_with_environment_suffix()
        values = model_version_with_env_suffix.split('_')
        environment_value: str = ""
        if len(values) >= 2:
            environment_value = values[1]
        self.assertEqual(environment_value, config.model_environment,
                         "environment value is not equal to config model environment")

    @patch('service.dynamo.jobrunner.DateUtils.get_current_time')
    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    def test_get_kpi_report_header_based_on_deployment_date_after_hours_on_day_of_deployment(
            self,
            persistency_manager_mock,
            current_time_mock
    ):
        # given
        deployment_date: datetime = datetime(2023, 12, 9, 15, 8, 13, 45678)

        deployment_detail = {
            'date': deployment_date.isoformat()
        }

        mock_response = MagicMock()
        mock_response.get_the_value.side_effect = [deployment_detail]
        persistency_manager_mock.return_value = mock_response
        current_time_mock.return_value = datetime(2023, 12, 9, 20, 8, 13)

        deployment_key: str = 'deployment_date_v2.2.2_US2'
        correlation_id: str = str(uuid.uuid4())
        # when
        model_deployment_detail = (ModelUtils.get_kpi_report_header_based_on_deployment_date(
            deployment_detail=deployment_detail,
            deployment_key=deployment_key,
            correlation_id=correlation_id
        ))
        # then
        self.assertEqual(model_deployment_detail['report_header_msg'], "5 hours", "Expecting 5 hours")

    @patch('service.dynamo.jobrunner.DateUtils.get_current_time')
    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    def test_get_kpi_report_header_based_on_deployment_date_after_an_hour_on_day_of_deployment(
            self,
            persistency_manager_mock,
            current_time_mock
    ):
        # given
        deployment_date: datetime = datetime(2023, 12, 9, 19, 8, 13, 45678)

        deployment_detail = {
            'date': deployment_date.isoformat()
        }

        mock_response = MagicMock()
        mock_response.get_the_value.side_effect = [deployment_detail]
        persistency_manager_mock.return_value = mock_response
        current_time_mock.return_value = datetime(2023, 12, 9, 20, 8, 13)

        deployment_key: str = 'deployment_date_v2.2.2_US2'
        correlation_id: str = str(uuid.uuid4())
        # when
        model_deployment_detail = (ModelUtils.get_kpi_report_header_based_on_deployment_date(
            deployment_detail=deployment_detail,
            deployment_key=deployment_key,
            correlation_id=correlation_id
        ))
        # then
        self.assertEqual(model_deployment_detail['report_header_msg'], "1 hour", "Expecting 1 hour")

    @patch('service.dynamo.jobrunner.DateUtils.get_current_time')
    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    def test_get_kpi_report_header_based_on_deployment_date_on_next_day_of_deployment(
            self,
            persistency_manager_mock,
            current_time_mock
    ):
        # given
        deployment_date: datetime = datetime(2023, 12, 9, 19, 8, 13, 45678)

        deployment_detail = {
            'date': deployment_date.isoformat()
        }

        mock_response = MagicMock()
        mock_response.get_the_value.side_effect = [deployment_detail]
        persistency_manager_mock.return_value = mock_response
        current_time_mock.return_value = datetime(2023, 12, 10, 19, 8, 13)

        deployment_key: str = 'deployment_date_v2.2.2_US2'
        correlation_id: str = str(uuid.uuid4())
        # when
        model_deployment_detail = (ModelUtils.get_kpi_report_header_based_on_deployment_date(
            deployment_detail=deployment_detail,
            deployment_key=deployment_key,
            correlation_id=correlation_id
        ))
        # then
        self.assertEqual(model_deployment_detail['report_header_msg'], "1 day ", "Expecting 1 day")

    @patch('service.dynamo.jobrunner.DateUtils.get_current_time')
    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    def test_get_kpi_report_header_based_on_deployment_date_after_2days_of_deployment(
            self,
            persistency_manager_mock,
            current_time_mock
    ):
        # given
        deployment_date: datetime = datetime(2023, 12, 9, 19, 8, 13, 45678)

        deployment_detail = {
            'date': deployment_date.isoformat()
        }

        mock_response = MagicMock()
        mock_response.get_the_value.side_effect = [deployment_detail]
        persistency_manager_mock.return_value = mock_response
        current_time_mock.return_value = datetime(2023, 12, 11, 23, 8, 13)

        deployment_key: str = 'deployment_date_v2.2.2_US2'
        correlation_id: str = str(uuid.uuid4())
        # when
        model_deployment_detail = (ModelUtils.get_kpi_report_header_based_on_deployment_date(
            deployment_detail=deployment_detail,
            deployment_key=deployment_key,
            correlation_id=correlation_id
        ))
        print(model_deployment_detail['report_header_msg'])
        # then
        self.assertEqual(model_deployment_detail['report_header_msg'], "2 days 4 hours", "Expecting 2 days")

    @patch('service.common.modelutils.DateUtils')
    @patch('service.common.modelutils.PersistencyManager.get_persistency_manager')
    def test_get_kpi_report_header_based_on_deployment_when_deployment_detail_is_none_then_upsert_deployment_detail(
            self,
            persistency_manager_mock,
            datetime_mock
    ):

        mock_response = MagicMock()
        mock_response.get_the_value.return_value = None
        mock_response.upsert_value.return_value = True
        persistency_manager_mock.return_value = mock_response
        deployment_key: str = 'deployment_date_v2.3.2_US2'
        correlation_id: str = str(uuid.uuid4())

        mock_datetime = datetime(2024, 1, 17, 12, 34, 56)
        current_datetime = datetime(2024, 1, 20, 12, 34, 56)
        datetime_mock.get_datetime_in_iso_format.return_value = mock_datetime.isoformat()
        datetime_mock.get_current_time.return_value = current_datetime
        expected_value = {'date': mock_datetime.isoformat()}
        logging_msg: dict = {
            'success_message': 'model detail is entered successfully',
            'error_message': 'model detail is not entered successfully',
            'param1': 'deployment'
        }
        # when
        ModelUtils.get_kpi_report_header_based_on_deployment_date(
            deployment_detail={},
            deployment_key=deployment_key,
            correlation_id=correlation_id)
        # then
        mock_response.upsert_value.assert_called_once_with(
            key=deployment_key,
            value=expected_value,
            correlation_id=correlation_id,
            ttl=TimeToLive.ONE_YEAR_TTL.value,
            logging_msg=logging_msg
        )

    def test_dependency_classes_method(self):
        self.assertTrue(hasattr(PersistencyManager, 'get_persistency_manager'))
        self.assertTrue(hasattr(PersistencyManager, 'upsert_value'))
        self.assertTrue(hasattr(DateUtils, "get_datetime_in_iso_format"))
        self.assertTrue(hasattr(DateUtils, "get_current_time"))


if __name__ == '__main__':
    unittest.main()