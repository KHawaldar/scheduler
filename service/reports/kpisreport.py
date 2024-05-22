""Report to calculate the recall and bypass."""
from service.common.errormessages import ErrorMessage
from service.configs import config
import uuid
import logging
from service.elasticsearch.kpiquery import KPIQuery
from concurdatascience.utility import (
    create_http_client, prepare_request_headers
)
from urllib.parse import urlparse


class KPIReport:
    """Generates the KPI report."""

    def __init__(
            self, logging_service_endpoint: str, request_max_retries: int = 2,
            request_req_timeout_per_try_ms: int = 60000,
            request_req_timeout_total_ms: int = 120000,
            sleep_time_after_initial_request_ms: int = 5000,
            sleep_time_between_checks_ms: int = 2000,
            max_wait_time_ms: int = 90000):
        """Creates an instance of the KPIReport class.
        Args:
            logging_service_endpoint (str): The base URL for the logging service
            request_max_retries (int): Max number of request retries performed by envoy.
            request_req_timeout_per_try_ms (int): Timeout per try in milliseconds.
            request_req_timeout_total_ms (int): Total timeout of a single request.
            sleep_time_after_initial_request_ms (int): How long should be the wait between the initial
                request and the first check.
            sleep_time_between_checks_ms (int): Sleep time (ms) between each check.
            max_wait_time_ms (int): Max timeout of the entire process.
        Raises:
            ValueError: If logging_service_endpoint is empty.
        """
        if not logging_service_endpoint:
            raise ValueError("logging service endpoint is null")

        self.logging_service_url: str = f"{logging_service_endpoint}/*:log-2/_search"
        self.request_max_retries: int = max(1, int(request_max_retries))
        self.request_req_timeout_per_try_ms: int = max(1000, int(request_req_timeout_per_try_ms))
        self.request_req_timeout_total_ms: int = max(2000, int(request_req_timeout_total_ms))
        self.sleep_time_after_initial_request_ms: int = max(1000, int(sleep_time_after_initial_request_ms))
        self.sleep_time_between_checks_ms: int = max(1000, int(sleep_time_between_checks_ms))
        self.max_wait_time_ms: int = max(1000, int(max_wait_time_ms))
        self.environment: str = config.environment

    @staticmethod
    def calc_local_recall(resp) -> float:
        """Calculate the local recall.
        Args:
          resp: The request which we are sending to query the elastic search
        Returns:
          :aram resp:
        """
        if 'aggregations' in resp \
                and 'pfc' in resp["aggregations"] \
                and 'buckets' in resp["aggregations"]["pfc"] \
                and 'auditor_fails_trained_entities' in resp["aggregations"]["pfc"]["buckets"] \
                and 'doc_count' in resp["aggregations"]["pfc"]["buckets"]["auditor_fails_trained_entities"] \
                and 'true_fails_trained_entities' in resp["aggregations"]["pfc"]["buckets"] \
                and 'doc_count' in resp["aggregations"]["pfc"]["buckets"]["true_fails_trained_entities"]:

            auditor_fails_trained_entities_doc_count = (
                resp)["aggregations"]["pfc"]["buckets"]["auditor_fails_trained_entities"]["doc_count"]
            if auditor_fails_trained_entities_doc_count > 0:
                return round(
                    100 * resp["aggregations"]["pfc"]["buckets"]["true_fails_trained_entities"]["doc_count"] /
                    auditor_fails_trained_entities_doc_count, 2)
            else:
                return 0
        else:
            return -1

    @staticmethod
    def calc_total_recall(resp) -> float:
        """Calculate the total recall.
        Args:
          resp: The request which we are sending to query the elastic search
        Returns:
           :param resp:
        """
        if 'aggregations' in resp \
                and 'pfc' in resp["aggregations"] \
                and 'buckets' in resp["aggregations"]["pfc"] \
                and 'auditor_fails' in resp["aggregations"]["pfc"]["buckets"] \
                and 'doc_count' in resp["aggregations"]["pfc"]["buckets"]["auditor_fails"] \
                and 'true_fails' in resp["aggregations"]["pfc"]["buckets"] \
                and 'doc_count' in resp["aggregations"]["pfc"]["buckets"]["true_fails"]:

            auditor_fails_doc_count = resp["aggregations"]["pfc"]["buckets"]["auditor_fails"]["doc_count"]
            if auditor_fails_doc_count > 0:
                return round(
                    100 * resp["aggregations"]["pfc"]["buckets"]["true_fails"]["doc_count"] /
                    auditor_fails_doc_count, 2)
            else:
                return 0
        else:
            return -1

    @staticmethod
    def calc_total_bypass(resp) -> float:
        """Calculate total bypass.
        Args:
          resp: The request which we are sending to query the elastic search
        Returns:
           :param resp:
        """
        if 'aggregations' in resp \
                and 'ml_audit' in resp["aggregations"] \
                and 'buckets' in resp["aggregations"]["ml_audit"] \
                and 'ml_audit_questions' in resp["aggregations"]["ml_audit"]["buckets"] \
                and 'total_ml_audit_questions' in resp["aggregations"]["ml_audit"]["buckets"]["ml_audit_questions"] \
                and 'pfc_bypass' in resp["aggregations"]["pfc"]["buckets"] \
                and 'doc_count' in resp["aggregations"]["pfc"]["buckets"]["pfc_bypass"]:

            total_ml_audit_questions = (
                resp)["aggregations"]["ml_audit"]["buckets"]["ml_audit_questions"]["total_ml_audit_questions"][
                "value"]
            if total_ml_audit_questions > 0:
                return round(
                    100 * resp["aggregations"]["pfc"]["buckets"]["pfc_bypass"]["doc_count"] /
                    total_ml_audit_questions, 2)
            else:
                return 0
        else:
            return -1

    @staticmethod
    def calc_local_bypass(resp) -> float:
        """Calculate the local bypass.
        Args:
          resp: The request which we are sending to query the elastic search
        Returns:
            :param resp:
        """
        if 'aggregations' in resp \
                and 'pfc' in resp["aggregations"] \
                and 'buckets' in resp["aggregations"]["pfc"] \
                and 'received_questions' in resp["aggregations"]["pfc"]["buckets"] \
                and 'doc_count' in resp["aggregations"]["pfc"]["buckets"]["received_questions"] \
                and 'pfc_bypass' in resp["aggregations"]["pfc"]["buckets"] \
                and 'doc_count' in resp["aggregations"]["pfc"]["buckets"]["pfc_bypass"]:

            received_questions_doc_count = resp["aggregations"]["pfc"]["buckets"]["received_questions"]["doc_count"]
            if received_questions_doc_count > 0:
                return round(
                    100 * resp["aggregations"]["pfc"]["buckets"]["pfc_bypass"]["doc_count"] /
                    received_questions_doc_count, 2)
            else:
                return 0
        else:
            return -1

    @staticmethod
    def calc_sample_rate(resp) -> float:
        """Calculate the sample rate.
        Args:
          resp: The request which we are sending to query the elastic search
        Returns:
            :param resp:
        """
        if 'aggregations' in resp \
                and 'pfc' in resp["aggregations"] \
                and 'buckets' in resp["aggregations"]["pfc"] \
                and 'received_questions' in resp["aggregations"]["pfc"]["buckets"] \
                and 'doc_count' in resp["aggregations"]["pfc"]["buckets"]["received_questions"] \
                and 'sampled_questions' in resp["aggregations"]["pfc"]["buckets"] \
                and 'doc_count' in resp["aggregations"]["pfc"]["buckets"]["sampled_questions"]:

            received_questions_doc_count = resp["aggregations"]["pfc"]["buckets"]["received_questions"]["doc_count"]
            if received_questions_doc_count > 0:
                return round(
                    100 * resp["aggregations"]["pfc"]["buckets"]["sampled_questions"]["doc_count"] /
                    received_questions_doc_count, 2)
            else:
                return 0
        else:
            return -1

    @staticmethod
    def get_auditor_fails_document(resp) -> int:
        """Gets the auditor fails document.
        Args:
          resp: The request which we are sending to query the elastic search
        Returns:
            :param resp:
       """
       if 'aggregations' in resp \
                and 'pfc' in resp["aggregations"] \
                and 'buckets' in resp["aggregations"]["pfc"] \
                and 'auditor_fails' in resp["aggregations"]["pfc"]["buckets"] \
                and 'doc_count' in resp["aggregations"]["pfc"]["buckets"]["auditor_fails"]:
            return resp["aggregations"]["pfc"]["buckets"]["auditor_fails"]["doc_count"]
        return -1

    @staticmethod
    def get_sampled_questions(resp) -> int:
        """Gets the number sampled questions.
        Args:
          resp: The request which we are sending to query the elastic search
        Returns:
            :param resp:
      """
      if 'aggregations' in resp \
                and 'pfc' in resp["aggregations"] \
                and 'buckets' in resp["aggregations"]["pfc"] \
                and 'sampled_questions' in resp["aggregations"]["pfc"]["buckets"] \
                and 'doc_count' in resp["aggregations"]["pfc"]["buckets"]["sampled_questions"]:
            return resp["aggregations"]["pfc"]["buckets"]["sampled_questions"]["doc_count"]
        return -1

    @staticmethod
    def set_status(total_recall: float) -> str:
        """Set the status of the total recall for the Slack message colors.
        Args:
          total_recall: the total recall percentage
        Returns:
            :param: status
        """
        status = "good"
        if total_recall < 98:
            status = "danger"
        elif 98 <= total_recall < 99:
            status = "warning"
        return status

    def report_payload(self, resp: dict) -> dict:
        """Report the payload.
        Args:
          resp: response from the elastic search
        Returns:
            :param resp:
        """
        total_recall: float = self.calc_total_recall(resp)
        local_recall: float = self.calc_local_recall(resp)
        total_bypass: float = self.calc_total_bypass(resp)
        local_bypass: float = self.calc_local_bypass(resp)
        auditor_fails_count: int = self.get_auditor_fails_document(resp)
        sampled_questions: int = self.get_sampled_questions(resp)
        if (total_recall == -1 or
                local_recall == -1 or
                total_bypass == -1 or
                local_bypass == -1 or
                auditor_fails_count == -1):
            payload: dict = {
                "error": ErrorMessage.KEY_ERROR1,
                "status": "danger"
            }
            return payload

        status: str = self.set_status(total_recall)
        payload: dict = {
            "kibana_kpis": (
                f"Total Recall: {str(total_recall)}% || Total Bypass: {str(total_bypass)}% || Local Recall: "
                f"{str(local_recall)}% || Local Bypass: {str(local_bypass)}%"),
            "status": status,
            "icbc_calculation_kpis": {
                "total_recall": str(total_recall),
                "local_recall": str(local_recall),
                "total_bypass": str(total_bypass),
                "local_bypass": str(local_bypass),
                "auditor_fails_count": str(auditor_fails_count),
                "sampled_questions": str(sampled_questions)
            }
        }
        return payload

    @staticmethod
    def is_url(url: str) -> bool:
        """Report the payload.
        Args:
            url: URL to validate
        Returns:
            boolean:
        """
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

    def query_es(self, kpi_input_request: dict, correlation_id: str = None) -> dict:
        """Query the elastic search.
        Args:
          kpi_input_request: The request which we are sending to query the elastic search
          correlation_id: correlation_id
        Returns:
          payload: dictionary
          {
            'total_recall': 10,
            'local_recall': 20,
            'total_bypass': 45,
            'local_bypass': 32
          }
        """
        payload: dict = {}
        try:
            if correlation_id is None:
                correlation_id: str = str(uuid.uuid4())
            # validating the elastic search url.
            result = self.is_url(self.logging_service_url)
            if result is False:
                raise ValueError(f"invalid url {self.logging_service_url}")
            kpiquery: KPIQuery = KPIQuery()
            query_res: dict = kpiquery.get_query(kpi_input_request)
            if 'validation_error' in query_res:
                raise ValueError(query_res['validation_error'])
            else:
                headers = prepare_request_headers(
                    correlation_id=correlation_id,
                    request_max_retries=self.request_max_retries,
                    request_req_timeout_per_try_ms=self.request_req_timeout_per_try_ms,
                    connection_close=False
                )
                timeout = (60.0, 120.0)
                http_client = create_http_client()
                resp = http_client.post(
                    url=self.logging_service_url,
                    json=query_res['query'],
                    headers=headers,
                    timeout=timeout
                )

        except Exception as ex:
            logging.error({ex},
                          extra={
                              'correlation_id': correlation_id,
                              'ds_object': {
                                  'message': f'{type(ex).__name__} - {ex}',
                                  'param1': 'scheduler'
                              }})

            return {'error': ex}
        if resp.status_code != 200:
            logging.error({resp},
                          extra={
                              'correlation_id': correlation_id,
                              'ds_object': {
                                  'message': f'error calling the {self.logging_service_url} ',
                                  'param1': 'scheduler'
                              }})

            return {'error': resp}
        else:
            payload = self.report_payload(resp.json())
            if 'error' not in payload:
                logging.info(
                   f"After calling the report payload {payload['kibana_kpis']}", extra={
                        'correlation_id': correlation_id,
                        'ds_object': {
                            'message': 'kpisreport',
                            'param1': 'scheduler'
                        }})
            else:
                logging.error(
                    f"Error after calling the report payload {payload['error']}", extra={
                        'correlation_id': correlation_id,
                        'ds_object': {
                            'message': 'kpisreport',
                            'param1': 'scheduler'
                        }})
        return payload