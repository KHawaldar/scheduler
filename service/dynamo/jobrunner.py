"""Job Runner.
This class triggers the kpi report generation based on job key exists or not.
If job key[job-dd-mm-yyyy] is already exists, and if job is completed, then
it will not trigger the job.
"""
import json
import os
import time
import uuid
import logging
from service.common.dateutils import DateUtils
from service.common.modelutils import ModelUtils
from service.endpoints import get_pfc_kpi_report
from datetime import datetime, timedelta
from service.configs import config
from service.dynamo.persistencymanager import PersistencyManager
from service.enums.timetoliveenum import TimeToLive
from service.enums.icbcstatus import IcbcStatus
from service.reports.icbcmanager import ICBCManager
from service.enums.jobstatus import JobStatus


class JobRunner:
    """Triggers the job."""

    def __init__(self):
        """Creates the instance of JobRunner."""
        self.t_24_hours_job_status = None
        self.t_7_days_job_status = None
        self.icbc_service_status = None

    @staticmethod
    def get_pod_name() -> str:
        """Gets the Persistent Manager.
        Args:
        Returns:
          Persistent Manager
        """
        return os.environ.get('POD_NAME')

    def trigger_the_job(self, key: str, deployment_detail: dict, correlation_id: str = None):
        """It triggers the job to get the report.
           It will insert with job_dictionary with key as 24-hours-<date> or 7-days-<date>
           {
           job_runner_name: pod1,
           initiated_time: time in datetime object
           }
           It reads the table with same key. Checks the job_runner_name and pod name from
           the environment. If both are same it calls the elastic search query 2 times.
           On first call, it will request for last 24 hours.
           On 2nd call, it will request for last 7 days.
           if any of the call fails, it treats as a fail, and will be re-executed on consequent call of
           scheduling.
        Args:
             key: key to be inserted. It is in the format of 24-hours-<date> or 7-days-<date>
             correlation_id: Correlation id
             deployment_detail: dict
        Returns:
                   dict:
        """
        report_type = ""
        time_unit = ""
        message_detail = ""
        if "24-hours" in key:
            self.t_24_hours_job_status = JobStatus.JOB_IN_PROGRESS.name
            report_type = "24"
            message_detail = "24 hours"
            time_unit = "h"
        if "7-days" in key:
            self.t_7_days_job_status = JobStatus.JOB_IN_PROGRESS.name
            report_type = "7"
            message_detail = "7 days"
            time_unit = "d"
        persistency_manager: PersistencyManager = PersistencyManager.get_persistency_manager(
            config.icbc_params_table_name)
        pod_name: str = JobRunner.get_pod_name()
        initiated_time: str = DateUtils.get_datetime_in_iso_format()

        job_dict: dict = {
            'job_runner_name': pod_name,
            'initiated_time': initiated_time
        }

        if correlation_id is None:
            correlation_id: str = str(uuid.uuid4())
        logging_msg: dict = {
            'success_message': f'job detail for {message_detail} entered successfully',
            'error_message': f'job detail for {message_detail} is not entered successfully',
            'param1': 'scheduler'
        }

        is_value_inserted: bool = persistency_manager.upsert_value(
            key=key, value=job_dict,
            correlation_id=correlation_id,
            ttl=TimeToLive.ONE_DAY_TTL.value,
            logging_msg=logging_msg
        )
        response = {}
        # is_value_inserted == False--> there is some issue to upsert it.
        if is_value_inserted is False:
            error = {
                'message': f'failed to insert for {key} task {job_dict}'
            }
            response['error'] = error
            if "24-hours" in key:
                self.t_24_hours_job_status = JobStatus.JOB_ABORTED.name
            if "7-days" in key:
                self.t_7_days_job_status = JobStatus.JOB_ABORTED.name
            return response

        # query the table for checking the pod name in table
        job_result: dict = persistency_manager.get_the_value(key=key, correlation_id=correlation_id)
        if job_result:
            stored_pod_name: str = job_result['job_runner_name']
            logging.info(
                json.dumps(job_result, default=str), extra={
                    'correlation_id': correlation_id,
                    'ds_object': {
                        'message': 'job_result',
                        'param1': 'scheduler'
                    }})
            if stored_pod_name == pod_name:
                # get the total recall for a time period ,i,e now - 24h
                request_object_dict: dict = {
                    'absolute_time_from_': report_type,
                    'time_unit_': time_unit,
                    'relative_time_from_': None,
                    'relative_time_to_': None,
                    'model_version': ModelUtils.get_model_version_with_environment_suffix()
                }
                response: dict = get_pfc_kpi_report(request_object_dict)
                if 'error' in response:
                    if "24-hours" in key:
                        self.t_24_hours_job_status = JobStatus.JOB_ABORTED.name
                    if "7-days" in key:
                        self.t_7_days_job_status = JobStatus.JOB_ABORTED.name
                    return response

                # get the total recall for a time period ,i,e now - 7d
                deployment_key: str = f'deployment_date_{ModelUtils.get_model_version_with_environment_suffix()}'

                model_deployment_detail: dict = ModelUtils.get_kpi_report_header_based_on_deployment_date(
                    deployment_key=deployment_key,
                    correlation_id=correlation_id,
                    deployment_detail=deployment_detail
                )

                #  log to kibana
                if "24-hours" in key:
                    logging.info(response.get("kibana_kpis"),
                                 extra={
                                     'correlation_id': correlation_id,
                                     'ds_object': {
                                         'message': f'Report of last {message_detail}',
                                         'param1': 'scheduler',
                                         'param2': response.get("status"),
                                         'number1': report_type
                                     }})
                if "7-days" in key:
                    logging.info(response.get("kibana_kpis"),
                                 extra={
                                     'correlation_id': correlation_id,
                                     'ds_object': {
                                         'message': f"Report of last {model_deployment_detail['report_header_msg']}",
                                         'param1': 'scheduler',
                                         'param2': response.get("status"),
                                         'number1': 168
                                     }})
                    # model should be checked for the performance only after 7 days of deployment of the model.#
                    if model_deployment_detail['day'] >= 7:
                        response: dict = (ICBCManager(response).check_the_model_performance_and_actioned_icbc_service(
                            correlation_id=correlation_id))

                        if 'error' not in response:
                            if response['message'] == 'pfc is in silent mode':
                                self.icbc_service_status = IcbcStatus.SILENT.name
                            if response['message'] == 'company recall':
                                self.icbc_service_status = IcbcStatus.ACTIVE.name
                        else:
                            response['t_7_days_job_status'] = JobStatus.JOB_ABORTED
                            self.t_7_days_job_status = JobStatus.JOB_ABORTED.name
                            return response

                completion_time: str = DateUtils.get_datetime_in_iso_format()
                job_dict: dict = {
                    'job_runner_name': pod_name,
                    'initiated_time': initiated_time,
                    'completion_time': completion_time
                }
                logging_msg: dict = {
                    'success_message': f'job detail for {message_detail} is updated successfully',
                    'error_message': f'job detail for {message_detail} is not entered successfully',
                    'param1': 'scheduler'
                }
                persistency_manager.upsert_value(
                    key=key, value=job_dict,
                    correlation_id=correlation_id,
                    ttl=TimeToLive.ONE_DAY_TTL.value,
                    logging_msg=logging_msg
                )
                if "24-hours" in key:
                    response['t_24_hours_job_status'] = JobStatus.JOB_COMPLETED
                    self.t_24_hours_job_status: str = JobStatus.JOB_COMPLETED.name
                if "7-days" in key:
                    response['t_7_days_job_status'] = JobStatus.JOB_COMPLETED
                    self.t_7_days_job_status: str = JobStatus.JOB_COMPLETED.name

                return response

    def check_job_executed_successfully(self, key: str, job_result: dict) -> str:
        """It checks the job executed successfully or not.
        Args:
            key: Unique key which checks the job status.
            job_result: job_result
        Returns:
            'COMPLETED' if job completed successfully.
            'JOB_IN_PROGRESS' if job is in progress.
            'JOB_ABORTED' if job is not completed within a time span of 10 minutes.
        """
        if 'initiated_time' not in job_result:
            raise ValueError(f'initiated time key not exists for the {key}')
        if 'initiated_time' in job_result:
            if 'initiated_time' in job_result and job_result['initiated_time'] is None:
                raise ValueError(f'initiated time is null for the {key}')

            if len(job_result['initiated_time'].strip()) == 0:
                raise ValueError(f'initiated time is empty for the {key}')
        else:
            raise ValueError(f'initiated time is not exists for the {key}')

        initiated_time: str = job_result['initiated_time']
        now: datetime = DateUtils.get_current_time()

        if 'completion_time' not in job_result:

            if now - datetime.fromisoformat(initiated_time) < timedelta(minutes=10):
                status = JobStatus.JOB_IN_PROGRESS.name
            else:
                status = JobStatus.JOB_ABORTED.name
        else:
            status = JobStatus.JOB_COMPLETED.name
        if '24-hours' in key:
            self.t_24_hours_job_status = status
        else:
            self.t_7_days_job_status = status
        return status

    def check_job_to_schedule(self):
        """It checks the job triggered or not.
        If not executed, it will trigger the job. If it fails to update the job detail in table,
        it will be re-executed on consequent call of scheduling.
        There are 2 calls to elastic search query. If any of the call fails,
        it treats as a fail, and will be re-executed on consequent call of
        scheduling.
        Args:
        Returns:
            True if the work was completed else False
        """
        # check the deployment date exists or not. if not reinsert it.
        correlation_id: str = str(uuid.uuid4())
        deployment_key: str = f'deployment_date_{ModelUtils.get_model_version_with_environment_suffix()}'
        deployment_detail: dict = ModelUtils.get_model_detail_from_db(correlation_id=correlation_id,
                                                                      deployment_key=deployment_key)
        if deployment_detail is None:
            deployment_detail = ModelUtils.insert_deployment_detail(
                correlation_id=correlation_id,
                key=deployment_key)

        # check the job status for 24 hours
        key: str = f'24-hours {"-"} {time.strftime("%d-%m-%Y")}'
        self.check_job_status(key=key, correlation_id=correlation_id, deployment_detail=deployment_detail)

        # check for job status for 7 days
        key = f'7-days {"-"} {time.strftime("%d-%m-%Y")}'
        self.check_job_status(key=key, correlation_id=correlation_id, deployment_detail=deployment_detail)

    def check_job_status(self, key: str, correlation_id: str, deployment_detail: dict):
        """It checks the job status, if not started, or aborted, it will trigger the job.
        Args:
            key: key
            correlation_id: correlation_id
            deployment_detail: dict
        """
        persistency_manager: PersistencyManager = PersistencyManager.get_persistency_manager(
            config.icbc_params_table_name)
        job_result: dict = persistency_manager.get_the_value(key=key, correlation_id=correlation_id)
        if job_result is None:
            self.trigger_the_job(key=key, deployment_detail=deployment_detail, correlation_id=correlation_id)
        else:
            status: str = self.check_job_executed_successfully(key=key, job_result=job_result)
            if status not in [JobStatus.JOB_COMPLETED.name, JobStatus.JOB_IN_PROGRESS.name]:
                self.trigger_the_job(key=key,
                                     deployment_detail=deployment_detail,
                                     correlation_id=correlation_id)