"""Model utils.
This module contains some utility functions that can help other components
of the service.
"""
from datetime import datetime
from service.common.dateutils import DateUtils
from service.configs import config
from service.dynamo.persistencymanager import PersistencyManager
from service.enums.timetoliveenum import TimeToLive


class ModelUtils:
    """ModelUtils."""

    @staticmethod
    def get_model_version_with_environment_suffix() -> str:
        """It gives model version with environment suffix.
        Args:
        Returns:
            str: model_version with environment suffix
        """
        return config.model_stable_version + "_" + config.model_environment

    @staticmethod
    def insert_deployment_detail(correlation_id: str, key: str) -> dict:
        """Gets the day or an hour from the date of deployment till the current date.
        Args:
            correlation_id: correlation_id
            key: key
        Returns: dict
        """
        persistency_manager: PersistencyManager = PersistencyManager.get_persistency_manager(
            config.icbc_params_table_name)

        deployment_detail = {'date': DateUtils.get_datetime_in_iso_format()}
        logging_msg: dict = {
            'success_message': 'model detail is entered successfully',
            'error_message': 'model detail is not entered successfully',
            'param1': 'deployment'
        }
        persistency_manager.upsert_value(
            key=key,
            value=deployment_detail,
            correlation_id=correlation_id,
            ttl=TimeToLive.ONE_YEAR_TTL.value,
            logging_msg=logging_msg
        )
        return deployment_detail

    @staticmethod
    def get_kpi_report_header_based_on_deployment_date(deployment_detail: dict,
                                                       deployment_key: str, correlation_id: str = None) -> dict:
        """Gets the day or an hour from the date of deployment till the current date.
        Args:
          deployment_detail: deployment_detail
          deployment_key: deployment key
          correlation_id: correlation_id
        Returns: dict
        """
        if deployment_detail is None or not deployment_detail:
            deployment_detail = ModelUtils.insert_deployment_detail(correlation_id=correlation_id, key=deployment_key)
        model_deployment_detail = {}

        deployment_date: str = deployment_detail['date']

        deployment_date_without_microseconds: str = deployment_date[:19]
        # take the difference of now and deployment date
        current_datetime: datetime = DateUtils.get_current_time()

        time_difference = current_datetime - datetime.fromisoformat(deployment_date_without_microseconds)
        model_deployment_detail['day'] = time_difference.days
        # get the hours

        hours, reminder_seconds = divmod(time_difference.seconds, 3600)
        report_header_msg: str = ""

        if 0 < time_difference.days <= 7:
            if time_difference.days == 1:
                day_label = "day"
            else:
                day_label = "days"
            report_header_msg = f'{time_difference.days} {day_label} '
        if time_difference.days <= 7 and hours > 0:
            if hours == 1:
                hour_label = "hour"
            else:
                hour_label = "hours"

            report_header_msg += f'{hours} {hour_label}'
        if time_difference.days > 7:
            report_header_msg = "7 days"
        model_deployment_detail['report_header_msg'] = report_header_msg
        return model_deployment_detail

    @staticmethod
    def get_model_detail_from_db(correlation_id, deployment_key):
        """Gets model detail from db.
        Args:
            deployment_key: deployment key
            correlation_id: correlation_id
        Returns: dict
        """
        deployment_detail: dict = PersistencyManager.get_persistency_manager(
            config.icbc_params_table_name).get_the_value(
            key=deployment_key,
            correlation_id=correlation_id)
        return deployment_detail