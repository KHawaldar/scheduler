"""Provides the sample rates."""
from service.common.dateutils import DateUtils
from service.common.modelutils import ModelUtils
from service.dynamo.persistencymanager import PersistencyManager
from service.enums.timetoliveenum import TimeToLive


class SampleRate:
    """Upsert the sample rates."""

    def __init__(self, table_name: str):
        """Creates the instance of PersistentManager.
        Args:
            table_name: Table which you want to insert/upsert/delete
        """
        self.persistency_manager: PersistencyManager = PersistencyManager.get_persistency_manager(
            table_name)

    def sameplerate_upsert(self, response: dict, correlation_id: str = None) -> dict:
        """Upsert the sample rate in datascience-icbc-params table.
        Args:
            response: Value to be inserted
            correlation_id: correlation id to track the transaction
        Returns:
              dict
        """
        sample_rate_key: str = "sample_rate_" + ModelUtils.get_model_version_with_environment_suffix()
        sample_rate_result: dict = SampleRate.get_sample_rate(self, correlation_id=correlation_id,
                                                              key=sample_rate_key)

        if sample_rate_result is None:
            sample_rate_dict = {
                '100': {
                    'date_added': DateUtils.get_datetime_in_iso_format(),
                    'total_recall': response['total_recall'],
                    'sampled_questions': response['sampled_questions'],
                    'auditor_fails_count': response['auditor_fails_count']
                }
            }
            logging_msg: dict = {
                'success_message': f'PFC went to silent mode due to low recall. '
                                   f'Total Recall: {response["total_recall"]} , '
                                   f' Auditor Fails: {response["auditor_fails_count"]}',

                'error_message': f'PFC should have entered into silent mode but the sample rate upsert failed. '
                                 f'Total Recall: {response["total_recall"]} ,'
                                 f' Auditor Fails: {response["auditor_fails_count"]}',
                'param1': response['total_recall'],
                'param2': response["auditor_fails_count"]
            }
            is_value_inserted: bool = self.persistency_manager.upsert_value(key=sample_rate_key,
                                                                            value=sample_rate_dict,
                                                                            correlation_id=correlation_id,
                                                                            ttl=TimeToLive.ONE_YEAR_TTL.value,
                                                                            logging_msg=logging_msg)
            result = {
                'key': sample_rate_key,
                'sample_rate': sample_rate_dict,
                'is_value_inserted': is_value_inserted
            }
        else:
            result = {
                'key': sample_rate_key,
                'sample_rate': sample_rate_result,
                'is_value_inserted': True
            }
        return result

    def get_sample_rate(self, correlation_id: str, key: str) -> dict:
        """Get the sample rate in datascience-icbc-params table.
        Args:
            correlation_id: str
            key: str
        Returns:
             sample_rate_result: dict
        """
        sample_rate_result: dict = self.persistency_manager.get_the_value(
            key=key, correlation_id=correlation_id)
        return sample_rate_result