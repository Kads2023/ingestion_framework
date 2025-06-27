import pytest
from unittest.mock import MagicMock


@pytest.fixture
def mock_instance():
    class MockIngest:
        this_class_name = "TestIngest"
        spark = MagicMock()
        job_args_obj = MagicMock()

    return MockIngest()


def test_quarantine_write_schema_evolution(monkeypatch, mock_instance):
    mock_df = MagicMock()

    mock_instance.job_args_obj.get.return_value = False  # dry_run = False
    mock_instance.job_args_obj.get_mandatory.return_value = "default.quarantine_table"

    mock_writer = MagicMock()
    mock_writer.option.return_value = MagicMock(saveAsTable=MagicMock())
    monkeypatch.setattr(mock_df.write, "mode", lambda mode: mock_writer)

    mock_instance.write_data_to_quarantine_table = __import__("types").MethodType(
        __import__("your_module").YourClass.write_data_to_quarantine_table, mock_instance
    )

    mock_instance.write_data_to_quarantine_table(mock_df)

    mock_writer.option.assert_called_with("mergeSchema", "true")


def test_quarantine_write_dry_run(monkeypatch, mock_instance):
    mock_df = MagicMock()

    mock_instance.job_args_obj.get.return_value = True  # dry_run = True
    mock_instance.job_args_obj.get_mandatory.return_value = "default.quarantine_table"

    # Prevent actual write call
    monkeypatch.setattr(mock_df.write, "mode", lambda mode: pytest.fail("Write should not be called in dry run"))

    mock_instance.write_data_to_quarantine_table = __import__("types").MethodType(
        __import__("your_module").YourClass.write_data_to_quarantine_table, mock_instance
    )

    # Should not raise or write
    mock_instance.write_data_to_quarantine_table(mock_df)


def test_quarantine_table_not_found(mock_instance):
    mock_df = MagicMock()

    mock_instance.job_args_obj.get.return_value = False
    mock_instance.job_args_obj.get_mandatory.side_effect = ValueError("quarantine_target_location is missing")

    mock_instance.write_data_to_quarantine_table = __import__("types").MethodType(
        __import__("your_module").YourClass.write_data_to_quarantine_table, mock_instance
    )

    with pytest.raises(ValueError, match="quarantine_target_location is missing"):
        mock_instance.write_data_to_quarantine_table(mock_df)
