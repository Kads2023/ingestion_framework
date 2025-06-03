import abc
import json


class BaseSchemaExtractor(abc.ABC):
    @abc.abstractmethod
    def __init__(self, **kwargs):
        pass

    @abc.abstractmethod
    def connect(self):
        """Establish a database connection."""
        pass

    @abc.abstractmethod
    def disconnect(self):
        """Close the database connection."""
        pass

    @abc.abstractmethod
    def extract_metadata(self, table_names: list[str], output_file: str):
        """
        Extract metadata for given tables and write to a JSON file.

        Args:
            table_names (list[str]): List of table names.
            output_file (str): File to save the extracted metadata as JSON.
        """
        pass

    def write_to_json(self, data: list[dict], output_file: str):
        """Helper method to write data to a JSON file."""
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"✅ Metadata written to {output_file}")
