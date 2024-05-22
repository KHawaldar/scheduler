import unittest
from unittest.mock import MagicMock, patch
from service.common.modelutils import ModelUtils
from service.dynamo.jobrunner import JobRunner
from service.dynamo.persistencymanager import PersistencyManager
from datetime import datetime
from service.enums.icbcstatus import IcbcStatus
from service.enums.jobstatus import JobStatus


class TestJobRunner(unittest.TestCase):

    @patch('service.common.modelutils.DateUtils')
    @patch('service.dynamo.jobrunner.ModelUtils.get_model_detail_from_db')
    @patch('service.dynamo.jobrunner.get_pfc_kpi_report')
    @patch('service.dynamo.jobrunner.JobRunner.get_pod_name')
    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    def test_job_when_pod_crashes_and_2nd_time_passed(
            self,
            persistency_manager_mock,
            pod_name_mock,
            report_mock,
            model_detail_mock,
            dateutils_mock
    ):
        mock_response = MagicMock()
        initiated_time: str = datetime(2023, 12, 12, 0, 0, 0).isoformat()
        completion_time: str = datetime(2023, 12, 12, 0, 0, 5).isoformat()
        deployment_date: str = datetime(2023, 12, 11, 15, 8, 13, 45678).isoformat()
        current_time: datetime = datetime(2023, 12, 12, 0, 15, 0)
        job_result_without_completion_time = {
            'job_runner_name': 'pod1',
            'initiated_time': initiated_time
        }
        deployment_detail = {
            'date': deployment_date
        }

        job_result_with_completion_time = {
            'job_runner_name': 'pod1',
            'initiated_time': initiated_time,
            'completion_time': completion_time
        }
        mock_response.get_the_value.side_effect = [job_result_without_completion_time,
                                                   job_result_without_completion_time,
                                                   job_result_without_completion_time,
                                                   job_result_without_completion_time,
                                                   deployment_detail,
                                                   job_result_with_completion_time]
        mock_response.upsert.return_value = True
        persistency_manager_mock.return_value = mock_response
        pod_name_mock.return_value = 'pod1'
        report_response_for_24hours = {
            'icbc_calculation_kpis': {
                'total_recall': '10',
                'local_recall': '20',
                'total_bypass': '15',
                'local_bypass': '20'
            },
            'status': 'danger',
            'kibana_kpis': {
                'Total Recall: 10% || Total Bypass: 15% || Local Recall: 20% || Local Bypass: 20%'
            }
        }
        report_response_for_7days = {
            'icbc_calculation_kpis': {
                'total_recall': '10',
                'local_recall': '20',
                'total_bypass': '15',
                'local_bypass': '20',
                'auditor_fails_count': '45'
            },
            'status': 'danger',
            'kibana_kpis': {
                'Total Recall: 10% || Total Bypass: 15% || Local Recall: 20% || Local Bypass: 20%'
            }

        }
        report_mock.side_effect = [report_response_for_24hours, report_response_for_7days]
        model_detail_mock.return_value = deployment_detail
        dateutils_mock.get_current_time.return_value = current_time
        jobrunner: JobRunner = JobRunner()
        jobrunner.check_job_to_schedule()
        self.assertEqual(jobrunner.t_24_hours_job_status, JobStatus.JOB_COMPLETED.name,
                         "job is not scheduled")  # add assertion here
        self.assertEqual(jobrunner.t_7_days_job_status, JobStatus.JOB_COMPLETED.name,
                         "job is not scheduled")  # add assertion here

    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    def test_job_already_scheduled(self, persistency_manager_mock):
        mock_response = MagicMock()
        initiated_time: str = datetime(2023, 12, 12, 0, 0, 0).isoformat()
        completion_time: str = datetime(2023, 12, 12, 0, 0, 5).isoformat()

        job_result = {
            'job_runner_name': 'pod1',
            'initiated_time': initiated_time,
            'completion_time': completion_time
        }
        mock_response.get_the_value.return_value = job_result
        persistency_manager_mock.return_value = mock_response
        jobrunner: JobRunner = JobRunner()

        jobrunner.check_job_to_schedule()
        jobrunner.trigger_the_job = MagicMock()
        jobrunner.trigger_the_job.assert_not_called()

    @patch('service.dynamo.jobrunner.ModelUtils.get_model_detail_from_db')
    @patch('service.dynamo.jobrunner.get_pfc_kpi_report')
    @patch('service.dynamo.jobrunner.JobRunner.get_pod_name')
    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    def test_job_schedule_of_the_job(
            self,
            persistency_manager_mock,
            pod_name_mock,
            report_mock,
            model_detail_mock):
        mock_response = MagicMock()
        initiated_time: str = datetime(2023, 12, 12, 0, 0, 0).isoformat()
        deployment_date: str = datetime(2023, 12, 9, 15, 8, 13, 45678).isoformat()
        job_result = {
            'job_runner_name': 'pod1',
            'initiated_time': initiated_time

        }
        deployment_detail = {
            'date': deployment_date
        }

        # when job_runner been called 2 times, first time, it returns
        # None and 2nd time, it returns job_result.
        mock_response.get_the_value.side_effect = [None, job_result, job_result, deployment_detail]
        persistency_manager_mock.return_value = mock_response
        pod_name_mock.return_value = "pod1"
        report_response_for_24hours = {
            'icbc_calculation_kpis': {
                'total_recall': '10',
                'local_recall': '20',
                'total_bypass': '15',
                'local_bypass': '20',
                'auditor_fails_count': '45'
            },
            'status': 'danger',
            'kibana_kpis': {
                'Total Recall: 10% || Total Bypass: 15% || Local Recall: 20% || Local Bypass: 20%'
            }
        }
        report_response_for_7days = {
            'icbc_calculation_kpis': {
                'total_recall': '95',
                'local_recall': '20',
                'total_bypass': '78',
                'local_bypass': '20',
                'auditor_fails_count': '50'
            },
            'status': 'danger',
            'kibana_kpis': {
                'Total Recall: 95% || Total Bypass: 78% || Local Recall: 20% || Local Bypass: 20%'
            }
        }
        report_mock.side_effect = [report_response_for_24hours, report_response_for_7days]
        model_detail_mock.return_value = deployment_detail

        jobrunner: JobRunner = JobRunner()
        jobrunner.trigger_the_job = MagicMock()
        jobrunner.trigger_the_job = MagicMock()

        jobrunner.check_job_to_schedule()
        jobrunner.trigger_the_job.assert_called()
        jobrunner.trigger_the_job.assert_called()

        self.assertEqual(jobrunner.trigger_the_job.call_count, 2,
                         "trigger_the_job")

    @patch('service.dynamo.jobrunner.ModelUtils.get_model_detail_from_db')
    @patch('service.dynamo.jobrunner.get_pfc_kpi_report')
    @patch('service.dynamo.jobrunner.JobRunner.get_pod_name')
    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    def test_job_when_trigger_the_24_hours_report_throws_error(
            self,
            persistency_manager_mock,
            pod_name_mock,
            report_mock,
            model_detail_mock
    ):
        mock_response = MagicMock()
        initiated_time: str = datetime(2023, 12, 12, 0, 0, 0).isoformat()

        job_result = {
            'job_runner_name': 'pod1',
            'initiated_time': initiated_time
        }
        # when job_runner been called 2 times,
        # first time, it returns  None
        # 2nd time, it returns job_result.
        mock_response.get_the_value.side_effect = [None, job_result, job_result, job_result]
        persistency_manager_mock.return_value = mock_response
        pod_name_mock.return_value = "pod1"

        deployment_date: str = datetime(2023, 12, 9, 15, 8, 13, 45678).isoformat()
        deployment_detail = {
            'date': deployment_date
        }

        model_detail_mock.return_value = deployment_detail
        report_response_for_24hours = {
            'error': 'exception'
        }
        report_response_for_7days = {
            'error': 'exception'
        }

        report_mock.side_effect = [report_response_for_24hours, report_response_for_7days]
        jobrunner: JobRunner = JobRunner()
        jobrunner.check_job_to_schedule()

        self.assertEqual(jobrunner.t_24_hours_job_status, JobStatus.JOB_ABORTED.name,
                         "job is not aborted")  # add assertion here
        self.assertEqual(jobrunner.t_7_days_job_status, JobStatus.JOB_ABORTED.name,
                         "job is not aborted")  # add assertion here

    @patch('service.dynamo.jobrunner.ModelUtils.get_model_detail_from_db')
    @patch('service.dynamo.jobrunner.get_pfc_kpi_report')
    @patch('service.dynamo.jobrunner.JobRunner.get_pod_name')
    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    def test_job_when_24_hours_success_and_7_days_throws_error_then_24_hours_completed_other_aborted(
            self,
            persistency_manager_mock,
            pod_name_mock,
            report_mock,
            model_detail_mock
    ):
        mock_response = MagicMock()
        initiated_time: str = datetime(2023, 12, 12, 0, 0, 0).isoformat()
        deployment_date: str = datetime(2023, 12, 1, 15, 8, 13, 45678).isoformat()

        job_result = {
            'job_runner_name': 'pod1',
            'initiated_time': initiated_time
        }
        deployment_result = {
            'date': deployment_date
        }
        # when job_runner been called 2 times,
        # first time, it returns  None
        # 2nd time, it returns job_result.
        mock_response.get_the_value.side_effect = [None, job_result, None, job_result, job_result, deployment_result]
        persistency_manager_mock.return_value = mock_response
        pod_name_mock.return_value = "pod1"
        report_response_for_24hours = {
            'total_recall': '10',
            'local_recall': '20',
            'total_bypass': '45',
            'local_bypass': '32'

        }
        report_response_for_7days = {
            'error': 'exception'
        }
        report_mock.side_effect = [report_response_for_24hours, report_response_for_7days]
        model_detail_mock.return_value = deployment_result
        jobrunner: JobRunner = JobRunner()
        jobrunner.check_job_to_schedule()
        self.assertEqual(jobrunner.t_24_hours_job_status, JobStatus.JOB_COMPLETED.name, "job is not completed")
        self.assertEqual(jobrunner.t_7_days_job_status, JobStatus.JOB_ABORTED.name,
                         "job is not aborted")  # add assertion here

    @patch('service.dynamo.jobrunner.JobRunner.get_pod_name')
    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    def test_when_24_hours_job_upserting_the_job_details_failed(
            self,
            persistency_manager_mock,
            pod_name_mock):
        mock_response = MagicMock()
        mock_response.get_the_value.return_value = None
        mock_response.upsert_value.return_value = False
        persistency_manager_mock.return_value = mock_response
        pod_name_mock.return_value = "pod1"
        jobrunner: JobRunner = JobRunner()
        jobrunner.check_job_to_schedule()
        self.assertEqual(jobrunner.t_24_hours_job_status, JobStatus.JOB_ABORTED.name,
                         "job is not aborted")  # add assertion here
        self.assertEqual(jobrunner.t_7_days_job_status, JobStatus.JOB_ABORTED.name,
                         "job is not aborted")  # add assertion here

    def test_get_persistency_manager(self):
        self.assertIsInstance(PersistencyManager.get_persistency_manager('dummy_table'), PersistencyManager)

    @patch('service.dynamo.jobrunner.ModelUtils.get_model_detail_from_db')
    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    def test_job_should_not_be_started_in_another_10_minute_if_job_already_started(
            self,
            persistency_manager_mock,
            model_detail_mock
    ):
        mock_response = MagicMock()
        initiated_time = datetime.today().isoformat()
        job_result_without_completion_time = {
            'job_runner_name': 'pod1',
            'initiated_time': initiated_time
        }
        deployment_date: str = datetime(2023, 12, 1, 15, 8, 13, 45678).isoformat()

        deployment_detail = {
            'date': deployment_date
        }

        mock_response.get_the_value.side_effect = [job_result_without_completion_time,
                                                   job_result_without_completion_time,
                                                   job_result_without_completion_time]

        persistency_manager_mock.return_value = mock_response
        model_detail_mock.return_value = deployment_detail
        jobrunner: JobRunner = JobRunner()
        jobrunner.check_job_to_schedule()
        self.assertEqual(jobrunner.t_24_hours_job_status, JobStatus.JOB_IN_PROGRESS.name, "job is already scheduled")
        self.assertEqual(jobrunner.t_7_days_job_status, JobStatus.JOB_IN_PROGRESS.name, "job is already scheduled")

    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    def test_check_job_executed_successfully_returns_job_in_progress(
            self,
            persistency_manager_mock):
        initiated_time = datetime.today().isoformat()
        job_result_without_completion_time = {
            'job_runner_name': 'pod1',
            'initiated_time': initiated_time
        }
        mock_response = MagicMock()
        mock_response.get_the_value.side_effect = [job_result_without_completion_time]
        persistency_manager_mock.return_value = mock_response
        key: str = "job-24-hours-" + datetime.today().date().isoformat()
        jobrunner: JobRunner = JobRunner()
        status = jobrunner.check_job_executed_successfully(key=key,
                                                           job_result=job_result_without_completion_time)
        self.assertEqual(status, "JOB_IN_PROGRESS", "job is not in progress")

    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    def test_check_task_executed_successfully_returns_job_completed(
            self,
            persistency_manager_mock):
        initiated_time = datetime(2024, 1, 5, 4, 8, 5).isoformat()
        completion_time = datetime(2024, 1, 5, 4, 8, 12).isoformat()
        job_result_with_completion_time = {
            'job_runner_name': 'pod1',
            'initiated_time': initiated_time,
            'completion_time': completion_time
        }
        mock_response = MagicMock()
        mock_response.get_the_value.side_effect = [job_result_with_completion_time]
        persistency_manager_mock.return_value = mock_response
        key: str = "job-24-hours-" + datetime.today().date().isoformat()
        jobrunner: JobRunner = JobRunner()
        status = jobrunner.check_job_executed_successfully(key=key, job_result=job_result_with_completion_time)
        self.assertEqual(status, "JOB_COMPLETED", "job is not completed")

    def test_check_job_executed_successfully_returns_job_aborted(
            self):
        initiated_time = datetime(2024, 1, 5, 4, 8, 5).isoformat()

        job_result_with_completion_time = {
            'job_runner_name': 'pod1',
            'initiated_time': initiated_time,

        }

        key: str = "job-24-hours-" + datetime.today().date().isoformat()
        jobrunner: JobRunner = JobRunner()
        status = jobrunner.check_job_executed_successfully(key=key, job_result=job_result_with_completion_time)
        self.assertEqual(status, "JOB_ABORTED", "job is not aborted")

    def test_check_job_executed_successfully_throws_value_error(
            self):
        # given

        key: str = "job-24-hours-" + datetime.today().date().isoformat()

        # when
        jobrunner: JobRunner = JobRunner()
        job_result: dict = {}

        with self.assertRaises(ValueError) as context:
            jobrunner.check_job_executed_successfully(key=key, job_result=job_result)
        self.assertEqual(str(context.exception), f"initiated time key not exists for the {key}")

        job_result_with_completion_time = {
            'job_runner_name': 'pod1',
            'initiated_time': None,
        }

        jobrunner: JobRunner = JobRunner()

        with self.assertRaises(ValueError) as context:
            jobrunner.check_job_executed_successfully(key=key,
                                                      job_result=job_result_with_completion_time)
        self.assertEqual(str(context.exception), f"initiated time is null for the {key}")

    def test_check_job_executed_successfully_throws_value_error_when_initiated_time_is_null(
            self):
        # given
        job_result_with_completion_time = {
            'job_runner_name': 'pod1',
            'initiated_time': None,
        }

        key: str = "job-24-hours-" + datetime.today().date().isoformat()
        jobrunner: JobRunner = JobRunner()

        with self.assertRaises(ValueError) as context:
            jobrunner.check_job_executed_successfully(key=key,
                                                      job_result=job_result_with_completion_time)
        self.assertEqual(str(context.exception), f"initiated time is null for the {key}")

    def test_check_job_executed_successfully_throws_value_error_when_there_is_no_initiated_time(
            self):
        # given
        job_result_with_completion_time = {
            'job_runner_name': 'pod1'
        }

        key: str = "job-24-hours-" + datetime.today().date().isoformat()
        jobrunner: JobRunner = JobRunner()

        with self.assertRaises(ValueError) as context:
            jobrunner.check_job_executed_successfully(key=key,
                                                      job_result=job_result_with_completion_time)
        self.assertEqual(str(context.exception), f"initiated time key not exists for the {key}")

    def test_check_job_executed_successfully_throws_error_when_there_is_no_value_initiated_time(
            self):
        # given
        job_result_with_completion_time = {
            'job_runner_name': 'pod1',
            'initiated_time': " "
        }

        key: str = "job-24-hours-" + datetime.today().date().isoformat()

        jobrunner: JobRunner = JobRunner()

        with self.assertRaises(ValueError) as context:
            jobrunner.check_job_executed_successfully(key=key,
                                                      job_result=job_result_with_completion_time)
        self.assertEqual(str(context.exception), f"initiated time is empty for the {key}")

    @patch('service.common.modelutils.ModelUtils.get_kpi_report_header_based_on_deployment_date')
    @patch('service.dynamo.jobrunner.get_pfc_kpi_report')
    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    @patch('service.common.dateutils.DateUtils.get_datetime_in_iso_format')
    @patch('service.dynamo.jobrunner.JobRunner.get_pod_name')
    def test_model_performance_should_not_be_checked_till_7th_day_of_new_model_deployment(
            self,
            pod_name_mock,
            datetime_iso_mock,
            persistency_manager_mock,
            report_mock,
            model_deployment_header_mock
    ):
        key: str = 'job-2024-01-24'
        pod_name_mock.return_value = 'pod1'
        jobrunner: JobRunner = JobRunner()
        jobrunner.check_the_model_performance = MagicMock()
        mock_response = MagicMock()
        initiated_time = datetime(2024, 1, 24, 23, 0, 21).isoformat()
        datetime_iso_mock.return_value = initiated_time
        deployment_date = datetime(2024, 1, 23, 19, 0, 21).isoformat()
        deployment_detail = {
            'date': deployment_date
        }
        model_deployment_detail = {
            'day': 1,
            'report_header_msg': '1 day 4 hours'
        }
        model_deployment_header_mock.return_value = model_deployment_detail

        job_result = {
            'job_runner_name': 'pod1',
            'initiated_time': initiated_time,

        }
        mock_response.get_the_value.side_effect = [job_result]
        mock_response.upsert_return_value = True
        persistency_manager_mock.return_value = mock_response
        report_response_for_24hours = {
            'icbc_calculation_kpis': {
                'total_recall': '10',
                'local_recall': '20',
                'total_bypass': '15',
                'local_bypass': '20',
                'auditor_fails_count': '45'
            },
            'status': 'danger',
            'kibana_kpis': {
                'Total Recall: 10% || Total Bypass: 15% || Local Recall: 20% || Local Bypass: 20%'
            }
        }
        report_response_for_7days = {
            'icbc_calculation_kpis': {
                'total_recall': '95',
                'local_recall': '20',
                'total_bypass': '78',
                'local_bypass': '20',
                'auditor_fails_count': '50'

            },
            'status': 'danger',
            'kibana_kpis': {
                'Total Recall: 95% || Total Bypass: 78% || Local Recall: 20% || Local Bypass: 20%'
            }
        }
        report_mock.side_effect = [report_response_for_24hours, report_response_for_7days]
        # when
        jobrunner.trigger_the_job(deployment_detail=deployment_detail, key=key)
        # then
        jobrunner.check_the_model_performance.assert_not_called()

    @patch('service.dynamo.samplerate.SampleRate.sameplerate_upsert')
    @patch('service.dynamo.jobrunner.ModelUtils.get_model_detail_from_db')
    @patch('service.dynamo.jobrunner.DateUtils.get_current_time')
    @patch('service.dynamo.jobrunner.get_pfc_kpi_report')
    @patch('service.dynamo.jobrunner.JobRunner.get_pod_name')
    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    def test_for_setting_the_service_in_silent_mode_from_scratch(
            self,
            persistency_manager_mock,
            pod_name_mock,
            report_mock,
            current_time_mock,
            model_detail_from_db_mock,
            sample_rate_mock
    ):
        mock_response = MagicMock()
        initiated_time: str = datetime(2023, 12, 18, 0, 0, 0).isoformat()
        deployment_date: str = datetime(2023, 12, 9, 15, 8, 13, 45678).isoformat()
        job_result = {
            'job_runner_name': 'pod1',
            'initiated_time': initiated_time,
            'model_version': ModelUtils.get_model_version_with_environment_suffix()
        }
        deployment_detail = {
            'date': deployment_date
        }

        # when job_runner been called 2 times, first time, it returns
        # None and 2nd time, it returns job_result.
        sample_rate_value = None
        mock_response.get_the_value.side_effect = [None, job_result, job_result, job_result, job_result,
                                                   deployment_detail,
                                                   sample_rate_value]

        persistency_manager_mock.return_value = mock_response
        persistency_manager_mock.upsert.return_value = True
        pod_name_mock.return_value = "pod1"
        report_response_for_24hours = {
            'icbc_calculation_kpis': {
                'total_recall': '10',
                'local_recall': '20',
                'total_bypass': '15',
                'local_bypass': '20',
                'auditor_fails_count': '300'
            },
            'status': 'danger',
            'kibana_kpis': {
                'Total Recall: 10% || Total Bypass: 15% || Local Recall: 20% || Local Bypass: 20%'
            }
        }
        report_response_for_7days = {
            'icbc_calculation_kpis': {
                'total_recall': '92.4',
                'local_recall': '20',
                'total_bypass': '78',
                'local_bypass': '20',
                'auditor_fails_count': '500',
                'sampled_questions': 8690
            },
            'status': 'danger',
            'kibana_kpis': {
                'Total Recall: 95% || Total Bypass: 78% || Local Recall: 20% || Local Bypass: 20%'
            }
        }
        report_mock.side_effect = [report_response_for_24hours, report_response_for_7days]
        current_time_mock.return_value = datetime(2023, 12, 18, 0, 30, 0)
        model_detail_from_db_mock.return_value = deployment_detail
        sample_rate_dict = {
            '100': {
                'date_added': '2024:01:01T12:01:12',
                'total_recall': 45,
                'sampled_questions': 56908,
                'auditor_fails_count': 980
            }
        }
        sample_rate_result = {
            'key': "sample_rate_v2_US2",
            'sample_rate': sample_rate_dict,
            'is_value_inserted': True
        }

        sample_rate_mock.return_value = sample_rate_result
        jobrunner: JobRunner = JobRunner()
        jobrunner.check_job_to_schedule()
        self.assertEqual(jobrunner.icbc_service_status, IcbcStatus.SILENT.name, "icbc is active")

    @patch('service.dynamo.jobrunner.ModelUtils.get_model_detail_from_db')
    @patch('service.dynamo.jobrunner.DateUtils.get_current_time')
    @patch('service.dynamo.jobrunner.get_pfc_kpi_report')
    @patch('service.dynamo.jobrunner.JobRunner.get_pod_name')
    @patch('service.dynamo.persistencymanager.PersistencyManager.get_persistency_manager')
    def test_jobrunner_when_check_the_model_performance_and_actioned_icbc_service_throws_error_then_job_status_aborted(
            self,
            persistency_manager_mock,
            pod_name_mock,
            report_mock,
            current_time_mock,
            model_detail_mock
    ):
        mock_response = MagicMock()
        initiated_time: str = datetime(2023, 12, 18, 0, 0, 0).isoformat()
        deployment_date: str = datetime(2023, 12, 9, 15, 8, 13, 45678).isoformat()
        job_result = {
            'job_runner_name': 'pod1',
            'initiated_time': initiated_time

        }
        deployment_result = {
            'date': deployment_date
        }

        # when job_runner been called 2 times, first time, it returns
        # None and 2nd time, it returns job_result.
        sample_rate_value = None
        mock_response.get_the_value.side_effect = [None, job_result, None, job_result, deployment_result,
                                                   sample_rate_value]

        persistency_manager_mock.return_value = mock_response
        persistency_manager_mock.upsert.return_value = True
        pod_name_mock.return_value = "pod1"
        report_response_for_24hours = {
            'icbc_calculation_kpis': {
                'total_recall': '10',
                'local_recall': '20',
                'total_bypass': '15',
                'local_bypass': '20',
                'auditor_fails_count': '300'
            },
            'status': 'danger',
            'kibana_kpis': {
                'Total Recall: 10% || Total Bypass: 15% || Local Recall: 20% || Local Bypass: 20%'
            }
        }
        report_response_for_7days = {
            'icbc_calculation_kpis': {
                'local_recall': '20',
                'total_bypass': '78',
                'local_bypass': '20',
                'auditor_fails_count': '500',
                'sampled_questions': 8690
            },
            'status': 'danger',
            'kibana_kpis': {
                'Total Recall: 95% || Total Bypass: 78% || Local Recall: 20% || Local Bypass: 20%'
            }
        }
        report_mock.side_effect = [report_response_for_24hours, report_response_for_7days]
        current_time_mock.return_value = datetime(2023, 12, 18, 0, 30, 0)
        model_detail_mock.return_value = deployment_result
        jobrunner: JobRunner = JobRunner()
        jobrunner.check_job_to_schedule()
        self.assertEqual(jobrunner.t_24_hours_job_status, JobStatus.JOB_COMPLETED.name, "Job is not completed")
        self.assertEqual(jobrunner.t_7_days_job_status, JobStatus.JOB_ABORTED.name, "Job is not aborted")


if __name__ == '__main__':
    unittest.main()