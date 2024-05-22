"""kpi elastic search query.
This method gets the query
"""


class KPIQuery:
    """Generates the KPI query."""

    def __init__(self):
        """Creates an instance of the KPIQuery class.
        Args:
            None:
        Raises:
            None:
        """
        self.TIMESTAMP: str = "@timestamp"
        self.METRIC_VALUE: str = "metric.value"
        self.METRIC_SERVICE: str = "metric.service"
        self.DAVINCI_DATA_TASK_EVENT: str = "davinci_data.task_event"
        self.METRIC_PATH: str = "metric.path"

    @staticmethod
    def append_to_validation(validation: dict, msg: str, count: int) -> [dict, int]:
        """Validate the request.
        Args:
            validation:
            msg: str
            count: int
        Returns:
          :[dict, int]
        """
        count = count + 1
        validation[count] = msg
        return [validation, count]

    @staticmethod
    def validate_input(request: dict) -> dict:
        """Validate the request.
        Args:
          request: The request which we are sending to query the elastic search
        Returns:
         :validation_result
        """
        validation_result: dict = {}
        # when the user gave relative_time_to_, they must provide relative_time_from_
        count: int = -1
        if 'relative_time_to_' in request and request['relative_time_to_'] is not None \
                and 'relative_time_from_' in request \
                and request['relative_time_from_'] is None:
            [validation_result, count] = (
                KPIQuery.append_to_validation(validation_result, 'Please provide the relative_time_from_', count))
        # User should not give both absolute_time_from and relative_time_from
        if 'absolute_time_from_' in request and request['absolute_time_from_'] is not None \
                and 'relative_time_from_' in request and request['relative_time_from_'] is not None:
            [validation_result, count] = (
                KPIQuery.append_to_validation(validation_result,
                                              "Don't give both absolute_time_from_ and relative_time_from_", count))

        if 'model_version' not in request:
            [validation_result, count] = (
                KPIQuery.append_to_validation(validation_result,
                                              "Please provide the model_version for the stable model",
                                              count))

        return validation_result

    def construct_query(self, request: dict) -> dict:
        """Construct the query.
        Args:
          request: The request which we are sending to query the elastic search
        Returns:
         :param request:
        """
        time_from: str = request['absolute_time_from_']
        time_from = request['relative_time_from_'] if time_from is None else time_from

        time_to: str = request['relative_time_to_'] \
            if 'relative_time_to_' in request and request['relative_time_to_'] else 'now'

        time_unit: str = request['time_unit_']
        time_unit = "" if request['relative_time_from_'] is not None else time_unit

        if request['relative_time_from_'] is None:
            from_clause: str = str(f'{time_to}-{time_from}{time_unit}')
        else:
            from_clause: str = str(f'{time_from}')

        to_clause: str = str(f'{time_to}')

        model_version: str = request['model_version']

        query: dict = \
            {
                "aggs": {
                    "ml_audit": {
                        "aggs": {
                            "total_ml_audit_questions": {
                                "sum": {
                                    "field": "datascience_data.number1"
                                }
                            }
                        },
                        "filters": {
                            "filters": {
                                "ml_audit_questions": {
                                    "bool": {
                                        "filter": [
                                            self.generate_time_range_filter(from_clause=from_clause,
                                                                            to_clause=to_clause),
                                            {
                                                "term": {
                                                    "datascience_data.message": "total audit questions parsed"
                                                }
                                            },
                                            {
                                                "term": {
                                                    "application": "datascience-ml-audit"
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    "pfc": {
                        "filters": {
                            "filters": {
                                "true_fails": {
                                    "bool": {
                                        "filter": [
                                            self.generate_time_range_filter(from_clause, to_clause),
                                            self.generate_metric_service_term("pfc_stable"),
                                            {
                                                "terms": {
                                                    str(self.DAVINCI_DATA_TASK_EVENT): [
                                                        "fail-fail",
                                                        "fail-pend",
                                                        "fail-none",
                                                        "fail-pwe",
                                                        "fail-99"
                                                    ]
                                                }
                                            },

                                            self.generate_metric_value_term(value=1),
                                            self.generate_metric_path_term(model_version=model_version)

                                        ]
                                    }
                                },
                                "true_fails_trained_entities": {
                                    "bool": {
                                        "must_not": {
                                            "term": {
                                                "datascience_data.param2": "entity_not_in_allow_list"
                                            }
                                        },
                                        "filter": [
                                            self.generate_time_range_filter(from_clause=from_clause,
                                                                            to_clause=to_clause),
                                            self.generate_metric_service_term("pfc_stable"),
                                            {
                                                "terms": {
                                                    str(self.DAVINCI_DATA_TASK_EVENT): [
                                                        "fail-fail",
                                                        "fail-pend",
                                                        "fail-none",
                                                        "fail-pwe",
                                                        "fail-99"
                                                    ]
                                                }
                                            },
                                            self.generate_metric_value_term(value=1),
                                            self.generate_metric_path_term(model_version=model_version)
                                        ]
                                    }
                                },
                                "pfc_bypass": {
                                    "bool": {
                                        "filter": [
                                            self.generate_time_range_filter(from_clause=from_clause,
                                                                            to_clause=to_clause),
                                            self.generate_metric_service_term("pfc_stable"),

                                            {
                                                "term": {
                                                    str(self.DAVINCI_DATA_TASK_EVENT): "pass-bypass"
                                                }
                                            }
                                        ]
                                    }
                                },
                                "auditor_fails_trained_entities": {
                                    "bool": {
                                        "must_not": {
                                            "term": {
                                                "datascience_data.param2": "entity_not_in_allow_list"
                                            }
                                        },
                                        "filter": [
                                            self.generate_time_range_filter(from_clause, to_clause),
                                            self.generate_metric_service_term(service_name="pfc_stable"),
                                            self.generate_davinci_data_task_event_terms(),
                                            self.generate_metric_value_term(value=1),
                                            self.generate_metric_path_term(model_version=model_version)
                                        ]
                                    }
                                },
                                "sampled_questions": {
                                    "bool": {
                                        "must": {
                                            "exists": {
                                                "field": str(self.DAVINCI_DATA_TASK_EVENT)
                                            }
                                        },
                                        "filter": [
                                            self.generate_time_range_filter(from_clause, to_clause),
                                            self.generate_metric_service_term(service_name="pfc_stable"),
                                            self.generate_metric_value_term(value=1),
                                            self.generate_metric_path_term(model_version=model_version)
                                        ]
                                    }
                                },
                                "received_questions": {
                                    "bool": {
                                        "must": {
                                            "exists": {
                                                "field": str(self.DAVINCI_DATA_TASK_EVENT)
                                            }
                                        },
                                        "filter": [
                                            self.generate_time_range_filter(from_clause, to_clause),
                                            self.generate_metric_service_term('pfc_stable'),
                                            self.generate_metric_path_term(model_version=model_version),
                                        ]
                                    }
                                },
                                "auditor_fails": {
                                    "bool": {
                                        "filter": [
                                            self.generate_time_range_filter(from_clause, to_clause),
                                            self.generate_metric_service_term('pfc_stable'),
                                            self.generate_davinci_data_task_event_terms(),
                                            self.generate_metric_value_term(value=1),
                                            self.generate_metric_path_term(model_version=model_version),
                                        ]
                                    }
                                },
                            }
                        }
                    }
                },
                "size": 0
            }
        return query

    def generate_metric_value_term(self, value: int) -> dict:
        """Generates the term for the metric value.
         Args:
            value: metric_value
        Returns:
            dict: Term for the metric service.
        """
        return {
            "term": {
                str(self.METRIC_VALUE): value
            }
        }

    def generate_metric_path_term(self, model_version: str) -> dict:
        """Generates the term for the metric path.
        Args:
            model_version: model_version.
        Returns:
            dict: Term for the metric service.
        """
        return {
            "term": {
                str(self.METRIC_PATH): model_version
            }
        }

    def generate_time_range_filter(self, from_clause: str, to_clause: str) -> dict:
        """Generates a time range filter.
        Args:
          from_clause: Start of the time range.
          to_clause: End of the time range.
        Returns:
            dict: Time range filter.
        """
        return {
            "range": {
                str(self.TIMESTAMP): {
                    "from": from_clause,
                    "to": to_clause
                }
            }
        }

    def generate_metric_service_term(self, service_name: str) -> dict:
        """Generates the term for the metric service.
        Args:
            service_name: Name of the metric service.
        Returns:
            dict: Term for the metric service.
        """
        return {
            "term": {
                str(self.METRIC_SERVICE): service_name
            }
        }

    def generate_davinci_data_task_event_terms(self) -> dict:
        """Generates the terms for Davinci Data Task Events.
        Returns:
            dict: Time range filter.
        """
        return {
            "terms": {
                str(self.DAVINCI_DATA_TASK_EVENT): [
                    "fail-fail",
                    "fail-pend",
                    "fail-none",
                    "fail-pwe",
                    "fail-99",
                    "pass-fail",
                    "pass-pend",
                    "pass-none",
                    "pass-pwe",
                    "pass-99"
                ]
            }
        }

    def get_query(self, request: dict) -> dict:
        """Generates the query if inputs are valid.
        Args:
          :param request: The request which we are sending to query the elastic search
        Returns:
          :response
        """
        validation_result = self.validate_input(request)
        response = {}
        if len(validation_result) == 0:
            response['query'] = self.construct_query(request)
        else:
            response['validation_error'] = validation_result

        return response