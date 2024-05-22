import logging
import os
import unittest

from concurlogging import setup_logging
from xmlrunner import XMLTestRunner

from tests import test_service_api, test_service_worker
from tests.service.dynamo import test_jobrunner, test_persistencymanager, test_samplerate
from tests.service.elasticsearch import test_kpiquery
from tests.service.reports import (test_kpisreport, test_icbcmanager)
from tests.service.common import test_modelutils
from tests.service import (
    test_api, test_application, test_configs,
    test_handlers, test_integration, test_processors, test_schemas,
    test_utils, test_endpoints)

# Setup the logging module
setup_logging('unit-tests', logging.CRITICAL)
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'  # Ignore INFO and DEBUG messages from tf

# Assemble the suite
suite_list = [
    unittest.TestLoader().loadTestsFromTestCase(test_service_api.TestServiceApi),
    unittest.TestLoader().loadTestsFromTestCase(test_integration.TestAPI),
    unittest.TestLoader().loadTestsFromTestCase(test_api.TestAPI),
    unittest.TestLoader().loadTestsFromTestCase(test_service_worker.TestServiceWorker),
    unittest.TestLoader().loadTestsFromTestCase(test_schemas.TestSchemas),
    unittest.TestLoader().loadTestsFromTestCase(test_configs.TestConfigs),
    unittest.TestLoader().loadTestsFromTestCase(test_utils.TestUtils),
    unittest.TestLoader().loadTestsFromTestCase(test_processors.TestProcessors),
    unittest.TestLoader().loadTestsFromTestCase(test_handlers.TestHandlers),
    unittest.TestLoader().loadTestsFromTestCase(test_application.TestApplication),
    unittest.TestLoader().loadTestsFromTestCase(test_endpoints.TestEndPoints),
    unittest.TestLoader().loadTestsFromTestCase(test_jobrunner.TestJobRunner),
    unittest.TestLoader().loadTestsFromTestCase(test_persistencymanager.TestPersistencyManager),
    unittest.TestLoader().loadTestsFromTestCase(test_kpiquery.TestKPIQuery),
    unittest.TestLoader().loadTestsFromTestCase(test_kpisreport.TestKPIReport),
    unittest.TestLoader().loadTestsFromTestCase(test_icbcmanager.TestICBCManager),
    unittest.TestLoader().loadTestsFromTestCase(test_samplerate.TestSampleRate),
    unittest.TestLoader().loadTestsFromTestCase(test_modelutils.TestModelUtils)
]

# Run the tests
with open('unit.xml', 'wb') as output:
    XMLTestRunner(output=output).run(unittest.TestSuite(suite_list))