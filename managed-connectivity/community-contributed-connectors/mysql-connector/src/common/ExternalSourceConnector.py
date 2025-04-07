from typing import Dict
from src.constants import EntryType
from pyspark.sql import DataFrame
from abc import ABC, abstractmethod

# Interface defines methods for connector to pass metadata to common connector logic
class IExternalSourceConnector(ABC):

    @abstractmethod
    def get_dataset(self, schema_name: str, entry_type: EntryType):
        """Returns db object data for given schema"""
        pass

    @abstractmethod
    def get_db_schemas(self) -> DataFrame:
        """Returns dataframe of schemas to extract objects from"""
        pass