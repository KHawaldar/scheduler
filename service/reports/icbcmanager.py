"""ICBC Manager checks the model performance at overall and company level."""
import logging
import uuid

from service.common.errormessages import ErrorMessage
from service.dynamo.samplerate import SampleRate
from service.configs import config


class ICBCManager:
    """Provides the sample rates."""

    def __init__(self, kpiresponse: dict):
        """Creates the ICBCManager instance.
        Args:
           kpiresponse: response
        Returns:
            None:
        """
        self.kpiresponse = kpiresponse

    @staticmethod
    def get_model_performance(response: dict) -> dict:
        """Checks the performance of the model.
        Args:
            response: dict
        Returns:
            dict
        Raises:
            ValueError
        """
        model_performance_impact = {}

        if 'total_recall' not in response or 'auditor_fails_count' not in response:
            raise ValueError(ErrorMessage.TOTAL_RECALL_AUDITOR_FAILS_COUNT_ERROR2)
        if response['total_recall'] is None or response['auditor_fails_count'] is None:
            raise ValueError(ErrorMessage.TOTAL_RECALL_AUDITOR_FAILS_COUNT_ERROR1)
        if 'total_recall' in response and 'auditor_fails_count' in response:
            if float(response['total_recall']) < 99.5 and float(response['auditor_fails_count']) >= 100:
                model_performance_impact['set_pfc_in_silent_mode'] = True
            else:
                model_performance_impact['set_pfc_in_silent_mode'] = False
        return model_performance_impact

    def check_the_model_performance_and_actioned_icbc_service(self, correlation_id: str = None) -> dict:
        """Checks the model performance.
        Args:
           correlation_id: correlation_id
        Returns: dict
        """
        if correlation_id is None:
            correlation_id: str = str(uuid.uuid4())
        model_performance: dict = {}

        if 'icbc_calculation_kpis' in self.kpiresponse:
            try:
                model_performance_impact = self.get_model_performance(self.kpiresponse['icbc_calculation_kpis'])
                if model_performance_impact['set_pfc_in_silent_mode'] is False:
                    logging.info("calling the company recall")
                    model_performance['message'] = 'company recall'

                elif model_performance_impact['set_pfc_in_silent_mode'] is True:
                    upsert_response = SampleRate(config.icbc_params_table_name).sameplerate_upsert(
                        response=self.kpiresponse['icbc_calculation_kpis'],
                        correlation_id=correlation_id)

                    if upsert_response['is_value_inserted'] is False:
                        sample_rate = upsert_response['sample_rate']
                        key = upsert_response['key']
                        model_performance['error'] = \
                            f'Sample rate is not inserted properly {sample_rate} for the key {key}'
                        logging.info(
                            sample_rate,
                            extra={
                                'correlation_id': correlation_id,
                                'ds_object': {
                                    'message': 'Sample rate insertion failed',
                                    'param1': 'scheduler',

                                }})
                        return model_performance

                    logging.info(
                        self.kpiresponse.get("icbc_calculation_kpis"),
                        extra={
                            'correlation_id': correlation_id,
                            'ds_object': {
                                'message': 'Sample rate is inserted successfully',
                                'param1': 'scheduler'
                            }})
                    model_performance['message'] = 'pfc is in silent mode'

            except ValueError as ex:
                logging.error(
                    self.kpiresponse,
                    extra={
                        'correlation_id': correlation_id,
                        'ds_object': {
                            'message': ErrorMessage.INPUT_MISSING_TOTAL_RECALL_OR_AUDITOR_FAILS_CNT_VALUE_ERROR1,
                            'param1': 'scheduler',

                        }})
                model_performance['error'] = ex

        else:
            logging.error(
                ErrorMessage.ICBC_CALCULATION_ERROR1,
                extra={
                    'correlation_id': correlation_id,
                    'ds_object': {
                        'message': ErrorMessage.ICBC_CALCULATION_ERROR1,
                        'param1': 'scheduler',
                    }})
            model_performance['error'] = ErrorMessage.ICBC_CALCULATION_ERROR1

        return model_performance