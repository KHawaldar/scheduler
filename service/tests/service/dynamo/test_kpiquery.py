import unittest
from service.elasticsearch.kpiquery import KPIQuery


class TestKPIQuery(unittest.TestCase):

    def setUp(self):
        self.kpiquery = KPIQuery()

    @staticmethod
    def get_ml_audit_questions_timestamp(query: dict, from_or_to: str):
        return query['aggs']['ml_audit']['filters']['filters']['ml_audit_questions']['bool']['filter'][0]['range'][
            '@timestamp'][str(from_or_to)]

    def test_get_query_when_last24h_given(self):
        request_object_dict = {
            'absolute_time_from_': 24,
            'time_unit_': 'h',
            'relative_time_from_': None,
            'relative_time_to_': None,
            'model_version': 'v2'
        }
        query_resp = self.kpiquery.get_query(request_object_dict)
        if 'query' in query_resp:
            query = query_resp['query']
            from_clause = self.get_ml_audit_questions_timestamp(query, 'from')
            to_clause = self.get_ml_audit_questions_timestamp(query, 'to')
            self.assertEqual(from_clause, 'now-24h', 'from clause is not equal to now-24h')
            self.assertEqual(to_clause, 'now', 'to clause is not equal to now')

    def test_get_query(self):
        request_object_dict = {
            'absolute_time_from_': None,
            'time_unit_': 'h',
            'relative_time_from_': '2023-12-08',
            'relative_time_to_': None,
            'model_version': 'v2'
        }
        query_resp = self.kpiquery.get_query(request_object_dict)
        expected_query = {
            'aggs': {
                'ml_audit': {
                    'aggs': {
                        'total_ml_audit_questions':
                            {'sum': {'field': 'datascience_data.number1'}}},
                    'filters': {
                        'filters': {
                            'ml_audit_questions': {
                                'bool': {
                                    'filter':
                                        [{'range': {'@timestamp': {'from': '2023-12-08', 'to': 'now'}}},
                                         {'term': {'datascience_data.message': 'total audit questions parsed'}},
                                         {'term': {'application': 'datascience-ml-audit'}}]}}}}}, 'pfc': {
                    'filters': {
                        'filters': {
                            'true_fails': {
                                'bool': {
                                    'filter': [{'range': {'@timestamp': {'from': '2023-12-08', 'to': 'now'}}},
                                               {'term': {'metric.service': 'pfc_stable'}}, {
                                                   'terms': {
                                                       'davinci_data.task_event': ['fail-fail', 'fail-pend',
                                                                                   'fail-none', 'fail-pwe',
                                                                                   'fail-99']}},
                                               {'term': {'metric.value': 1}}, {'term': {'metric.path': 'v2'}}]}},
                            'true_fails_trained_entities': {
                                'bool': {
                                    'must_not': {'term': {'datascience_data.param2': 'entity_not_in_allow_list'}},
                                    'filter': [{'range': {'@timestamp': {'from': '2023-12-08', 'to': 'now'}}},
                                               {'term': {'metric.service': 'pfc_stable'}}, {
                                                   'terms': {
                                                       'davinci_data.task_event': ['fail-fail', 'fail-pend',
                                                                                   'fail-none', 'fail-pwe',
                                                                                   'fail-99']}},
                                               {'term': {'metric.value': 1}}, {'term': {'metric.path': 'v2'}}]}},
                            'pfc_bypass': {
                                'bool': {
                                    'filter': [{'range': {'@timestamp': {'from': '2023-12-08', 'to': 'now'}}},
                                               {'term': {'metric.service': 'pfc_stable'}},
                                               {'term': {'davinci_data.task_event': 'pass-bypass'}}]}},
                            'auditor_fails_trained_entities': {
                                'bool': {
                                    'must_not': {'term': {'datascience_data.param2': 'entity_not_in_allow_list'}},
                                    'filter': [{'range': {'@timestamp': {'from': '2023-12-08', 'to': 'now'}}},
                                               {'term': {'metric.service': 'pfc_stable'}}, {
                                                   'terms': {
                                                       'davinci_data.task_event': ['fail-fail', 'fail-pend',
                                                                                   'fail-none', 'fail-pwe', 'fail-99',
                                                                                   'pass-fail', 'pass-pend',
                                                                                   'pass-none', 'pass-pwe',
                                                                                   'pass-99']}},
                                               {'term': {'metric.value': 1}}, {'term': {'metric.path': 'v2'}}]}},
                            'sampled_questions': {
                                'bool': {
                                    'must': {'exists': {'field': 'davinci_data.task_event'}},
                                    'filter': [{'range': {'@timestamp': {'from': '2023-12-08', 'to': 'now'}}},
                                               {'term': {'metric.service': 'pfc_stable'}},
                                               {'term': {'metric.value': 1}}, {'term': {'metric.path': 'v2'}}]}},
                            'received_questions': {
                                'bool': {
                                    'must': {'exists': {'field': 'davinci_data.task_event'}},
                                    'filter': [{'range': {'@timestamp': {'from': '2023-12-08', 'to': 'now'}}},
                                               {'term': {'metric.service': 'pfc_stable'}},
                                               {'term': {'metric.path': 'v2'}}]}}, 'auditor_fails': {
                                'bool': {
                                    'filter': [{'range': {'@timestamp': {'from': '2023-12-08', 'to': 'now'}}},
                                               {'term': {'metric.service': 'pfc_stable'}}, {
                                                   'terms': {
                                                       'davinci_data.task_event': ['fail-fail', 'fail-pend',
                                                                                   'fail-none', 'fail-pwe', 'fail-99',
                                                                                   'pass-fail', 'pass-pend',
                                                                                   'pass-none', 'pass-pwe',
                                                                                   'pass-99']}},
                                               {'term': {'metric.value': 1}}, {'term': {'metric.path': 'v2'}}]}}}}}},
            'size': 0}
        self.assertEqual(query_resp['query'], expected_query, "query is not equal")

    def test_get_query_when_last_7days_given(self):
        request_object_dict = {
            'absolute_time_from_': 7,
            'time_unit_': 'd',
            'relative_time_from_': None,
            'relative_time_to_': None,
            'model_version': 'v2.3.2_EU2'
        }
        query_resp = self.kpiquery.get_query(request_object_dict)
        expected_query = {
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
                                        {
                                            "range": {
                                                "@timestamp": {
                                                    "from": "now-7d",
                                                    "to": "now"
                                                }
                                            }
                                        },
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
                                        {
                                            "range": {
                                                "@timestamp": {
                                                    "from": "now-7d",
                                                    "to": "now"
                                                }
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.service": "pfc_stable"
                                            }
                                        },
                                        {
                                            "terms": {
                                                "davinci_data.task_event": [
                                                    "fail-fail",
                                                    "fail-pend",
                                                    "fail-none",
                                                    "fail-pwe",
                                                    "fail-99"
                                                ]
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.value": 1
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.path": "v2.3.2_EU2"
                                            }
                                        }
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
                                        {
                                            "range": {
                                                "@timestamp": {
                                                    "from": "now-7d",
                                                    "to": "now"
                                                }
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.service": "pfc_stable"
                                            }
                                        },
                                        {
                                            "terms": {
                                                "davinci_data.task_event": [
                                                    "fail-fail",
                                                    "fail-pend",
                                                    "fail-none",
                                                    "fail-pwe",
                                                    "fail-99"
                                                ]
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.value": 1
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.path": "v2.3.2_EU2"
                                            }
                                        }
                                    ]
                                }
                            },
                            "pfc_bypass": {
                                "bool": {
                                    "filter": [
                                        {
                                            "range": {
                                                "@timestamp": {
                                                    "from": "now-7d",
                                                    "to": "now"
                                                }
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.service": "pfc_stable"
                                            }
                                        },
                                        {
                                            "term": {
                                                "davinci_data.task_event": "pass-bypass"
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
                                        {
                                            "range": {
                                                "@timestamp": {
                                                    "from": "now-7d",
                                                    "to": "now"
                                                }
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.service": "pfc_stable"
                                            }
                                        },
                                        {
                                            "terms": {
                                                "davinci_data.task_event": [
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
                                        },
                                        {
                                            "term": {
                                                "metric.value": 1
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.path": "v2.3.2_EU2"
                                            }
                                        }
                                    ]
                                }
                            },
                            "sampled_questions": {
                                "bool": {
                                    "must": {
                                        "exists": {
                                            "field": "davinci_data.task_event"
                                        }
                                    },
                                    "filter": [
                                        {
                                            "range": {
                                                "@timestamp": {
                                                    "from": "now-7d",
                                                    "to": "now"
                                                }
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.service": "pfc_stable"
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.value": 1
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.path": "v2.3.2_EU2"
                                            }
                                        }
                                    ]
                                }
                            },
                            "received_questions": {
                                "bool": {
                                    "must": {
                                        "exists": {
                                            "field": "davinci_data.task_event"
                                        }
                                    },
                                    "filter": [
                                        {
                                            "range": {
                                                "@timestamp": {
                                                    "from": "now-7d",
                                                    "to": "now"
                                                }
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.service": "pfc_stable"
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.path": "v2.3.2_EU2"
                                            }
                                        }
                                    ]
                                }
                            },
                            "auditor_fails": {
                                "bool": {
                                    "filter": [
                                        {
                                            "range": {
                                                "@timestamp": {
                                                    "from": "now-7d",
                                                    "to": "now"
                                                }
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.service": "pfc_stable"
                                            }
                                        },
                                        {
                                            "terms": {
                                                "davinci_data.task_event": [
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
                                        },
                                        {
                                            "term": {
                                                "metric.value": 1
                                            }
                                        },
                                        {
                                            "term": {
                                                "metric.path": "v2.3.2_EU2"
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            "size": 0
        }

        self.assertEqual(query_resp['query'], expected_query, "query is not equal")

    def test_get_query_when_relative_time_from_given(self):
        request_object_dict = {
            'absolute_time_from_': None,
            'time_unit_': 'h',
            'relative_time_from_': '2023-12-08',
            'relative_time_to_': None,
            'model_version': 'v2'
        }
        query_resp = self.kpiquery.get_query(request_object_dict)
        if 'query' in query_resp:
            query = query_resp['query']
            from_clause = self.get_ml_audit_questions_timestamp(query, 'from')
            to_clause = self.get_ml_audit_questions_timestamp(query, 'to')
            self.assertEqual(from_clause, '2023-12-08', 'from clause is not equal to 2023-12-08')
            self.assertEqual(to_clause, 'now', 'to clause is not equal to now')

    def test_get_query_when_relative_time_to_given(self):
        request_object_dict = {
            'absolute_time_from_': None,
            'time_unit_': 'h',
            'relative_time_from_': '2023-12-08',
            'relative_time_to_': '2023-12-10',
            'model_version': 'v2'
        }
        query_resp = self.kpiquery.get_query(request_object_dict)
        if 'query' in query_resp:
            query = query_resp['query']
            from_clause = self.get_ml_audit_questions_timestamp(query, 'from')
            to_clause = self.get_ml_audit_questions_timestamp(query, 'to')
            self.assertEqual(from_clause, '2023-12-08', 'from clause is not equal to 2023-12-08')
            self.assertEqual(to_clause, '2023-12-10', 'to clause is not equal to 2023-12-10')

    def test_get_query_raise_exception_when_absolute_and_relative_time_both_given(self):
        request_object_dict = {
            'absolute_time_from_': 24,
            'time_unit_': 'h',
            'relative_time_from_': '2023-12-08',
            'relative_time_to_': None,
            'model_version': 'v2'
        }
        query_resp = self.kpiquery.get_query(request_object_dict)
        if 'validation_error' in query_resp:
            validation = query_resp['validation_error']
            self.assertEqual(
                validation[0], "Don't give both absolute_time_from_ and relative_time_from_",
                "both absolute_time_from_ and relative_time_from_ were given.")

    def test_get_query_raise_exception_when_relative_time_to_given_but_no_relative_time_from(self):
        request_object_dict = {
            'absolute_time_from_': 24,
            'time_unit_': 'h',
            'relative_time_from_': None,
            'relative_time_to_': '2023-12-08',
            'model_version': 'v2'
        }
        query_resp = self.kpiquery.get_query(request_object_dict)
        if 'validation_error' in query_resp:
            validation = query_resp['validation_error']
            self.assertEqual(
                validation[0], "Please provide the relative_time_from_",
                "relative_time_from_not_given")

    def test_model_version_term(self):
        expected_value: dict = {"term": {"metric.path": "v2.3.2"}}
        self.assertEqual(KPIQuery.generate_metric_path_term(self.kpiquery, model_version="v2.3.2"), expected_value,
                         "values are not same")

    def test_time_range_filter(self):
        from_clause_value = "2023-01-01"
        to_clause_value = "2023-12-31"
        expected_result = {
            "range": {
                "@timestamp": {
                    "from": from_clause_value,
                    "to": to_clause_value
                }
            }
        }
        self.assertEqual(KPIQuery.generate_time_range_filter(self.kpiquery, from_clause=from_clause_value,
                                                             to_clause=to_clause_value),
                         expected_result, "values are not same")

    def test_generate_metric_service_term(self):
        expected_result = {
            "term": {
                "metric.service": "pfc_stable"
            }
        }
        self.assertEqual(KPIQuery.generate_metric_service_term(self.kpiquery, "pfc_stable"),
                         expected_result, "values are not same")

    def test_generate_davinci_data_task_event_terms(self):
        expected_result = {
            "terms": {
                str(self.kpiquery.DAVINCI_DATA_TASK_EVENT): [
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
        self.assertEqual(KPIQuery.generate_davinci_data_task_event_terms(self.kpiquery), expected_result,
                         "values are not same")

    def test_metric_value_one_term(self):
        expected_value = {"term": {"metric.value": 1}}
        self.assertEqual(KPIQuery.generate_metric_value_term(self.kpiquery, value=1), expected_value,
                         "values are not the same")


if __name__ == '__main__':
    unittest.main()