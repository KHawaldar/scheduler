"""Service Configuration Module.
This module contains the service configs and constants. Most of them can be
overwritten using model_environment variables.
Example:
    from service.configs import config
    print(config.service_name)
Attributes:
    config (ServiceConfig): A copy of this service configuration.
"""
import os
from dataclasses import dataclass, field
from distutils.util import strtobool


@dataclass(frozen=True, order=True)
class ServiceConfig:
    """Global service configurations.
    This object is frozen, can not be modified after it is created.
    """

    # The standardized service name.
    service_name: str = os.environ.get('SERVICE_NAME', 'icbc')

    # Parameters for the health checker endpoint bind
    health_check_parameters: dict = field(default_factory=lambda: {
        "processes": {
            b'service_api': 1,
            b'service_worker': 4,
            b'service_scheduler': 1
        }
    })

    # Name of the model
    model_name: str = os.environ.get('MODEL_NAME', 'icbc')

    # Where to write the exception if one of the main components terminates
    termination_log_path: str = os.environ.get('TERMINATION_LOG_PATH', None)

    # Max number of worker threads
    num_worker_threads: int = int(os.environ.get('NUM_WORKER_THREADS', 16))

    # Results table name
   

    # datascience_icbc_params table name
    icbc_params_table_name: str = os.environ.get('ICBC_PARAMS_TABLE_NAME', 'datascience-icbc-params')

 

   
    # Flag to bypass results of the service prediction
  
# Global config object
config = ServiceConfig()