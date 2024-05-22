"""Manages the given table operation."""
import uuid
from concurdatascience.dynamo import get_dynamo
import logging


class PersistencyManager:
    """Manages the given table insert, update, delete."""

    def __init__(self, table: str):
        """Creates the instance of PersistentManager.
        Args:
            table: Table which you want to insert/upsert/delete
        Raises:
            ValueError: If logging_service_endpoint is empty.
        """
        if not table:
            raise ValueError("Please give the table name")
        self.table = table

    @staticmethod
    def get_persistency_manager(table_name: str) -> 'PersistencyManager':
        """Gets the Persistency Manager.
        Args:
            table_name
        Returns:
          Persistent Manager
        """
        return PersistencyManager(table_name)

    def get_the_value(self, key: str, correlation_id: str = None):
        """This method reads the value for a given key.
        Args:
            key: key of the table.
            correlation_id: Correlation id
        Returns:
            For a given key, it returns the value.
        """
        if correlation_id is None:
            correlation_id: str = str(uuid.uuid4())
        try:
            return get_dynamo(self.table).get(key, correlation_id=correlation_id)
        except Exception as exc:
            logging.error(
                'Fatal error in the querying the table',
                extra={
                    'correlation_id': correlation_id,
                    'ds_object': {
                        'message': f'{type(exc).__name__} - {exc}'}})
            return None

    def upsert_value(self, key: str, value: dict, logging_msg: dict, correlation_id: str = None,
                     ttl: int = 86400, ) -> bool:
        """This method updates the value for a given key.
        Args:
            key: key of the table.
            value: value which should be inserted against the key
            logging_msg: msg to be logged
            correlation_id: identifies the unique transaction
            ttl: time to live
        Returns:
            bool
        """
        if correlation_id is None:
            correlation_id: str = str(uuid.uuid4())

        param1: str = logging_msg['param1'] if 'param1' in logging_msg else self.table
        param2: str = logging_msg['param2'] if 'param2' in logging_msg else key

        result: bool = get_dynamo(self.table).upsert(
            key=key, value=value,
            correlation_id=correlation_id,
            ttl=ttl)

        if result:
            success_message: str = logging_msg['success_message'] if 'success_message' in logging_msg \
                else 'Key-value upsert to DynamoDB table succeeded'

            logging.info(
                value, extra={
                    'correlation_id': correlation_id,
                    'ds_object': {
                        'message': success_message,
                        'param1': param1,
                        'param2': param2
                    }})
        else:
            error_message: str = logging_msg['error_message'] if 'error_message' in logging_msg \
                else 'Key-value upsert to DynamoDB table failed'

            logging.error(
                value, extra={
                    'correlation_id': correlation_id,
                    'ds_object': {
                        'message': error_message,
                        'param1': param1,
                        'param2': param2
                    }})

        return result