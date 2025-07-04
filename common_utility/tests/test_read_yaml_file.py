import pytest
from unittest.mock import mock_open, MagicMock


# Dummy class simulating your actual class
class DummyClass:
    def __init__(self):
        self.this_class_name = "DummyClass"
        self.lc = MagicMock()
        self.validate_function_param = MagicMock()

    def read_yaml_file(self, passed_file_path):
        import yaml  # simulate inline import

        this_module = f"[{self.this_class_name}.read_yaml_file()] -"
        self.validate_function_param(
            this_module,
            {
                "passed_file_path": {
                    "input_value": passed_file_path,
                    "data_type": "str",
                    "check_empty": True,
                },
            },
        )
        try:
            with open(passed_file_path, "r") as file_data:
                ret_dict = yaml.safe_load(file_data)
                return ret_dict
        except FileNotFoundError as fnf:
            self.lc.logger.error(f"{this_module} FileNotFoundError: {fnf}")
            raise
        except yaml.YAMLError as ye:
            self.lc.logger.error(f"{this_module} YAMLError: {ye}")
            raise
        except Exception as e:
            self.lc.logger.error(f"{this_module} Unexpected Exception: {e}")
            raise


@pytest.fixture
def dummy():
    return DummyClass()


def test_read_yaml_file_success(dummy, monkeypatch):
    monkeypatch.setattr("builtins.open", mock_open(read_data="key: value"))

    mock_yaml = MagicMock()
    mock_yaml.safe_load.return_value = {"key": "value"}

    monkeypatch.setitem(__import__("sys").modules, "yaml", mock_yaml)

    result = dummy.read_yaml_file("good.yaml")
    assert result == {"key": "value"}


def test_read_yaml_file_file_not_found(dummy, monkeypatch):
    monkeypatch.setattr("builtins.open", lambda *args, **kwargs: (_ for _ in ()).throw(FileNotFoundError("not found")))

    mock_yaml = MagicMock()
    monkeypatch.setitem(__import__("sys").modules, "yaml", mock_yaml)

    with pytest.raises(FileNotFoundError, match="not found"):
        dummy.read_yaml_file("missing.yaml")

    dummy.lc.logger.error.assert_called_once()
    assert "FileNotFoundError" in dummy.lc.logger.error.call_args[0][0]


def test_read_yaml_file_yaml_error(dummy, monkeypatch):
    monkeypatch.setattr("builtins.open", mock_open(read_data="invalid: :::"))

    class MockYAMLError(Exception):
        pass

    mock_yaml = MagicMock()
    mock_yaml.safe_load.side_effect = MockYAMLError("bad yaml")
    mock_yaml.YAMLError = MockYAMLError

    monkeypatch.setitem(__import__("sys").modules, "yaml", mock_yaml)

    with pytest.raises(MockYAMLError, match="bad yaml"):
        dummy.read_yaml_file("bad.yaml")

    dummy.lc.logger.error.assert_called_once()
    assert "YAMLError" in dummy.lc.logger.error.call_args[0][0]


def test_read_yaml_file_unexpected_error(dummy, monkeypatch):
    monkeypatch.setattr("builtins.open", lambda *args, **kwargs: (_ for _ in ()).throw(Exception("Boom")))

    mock_yaml = MagicMock()
    monkeypatch.setitem(__import__("sys").modules, "yaml", mock_yaml)

    with pytest.raises(Exception, match="Boom"):
        dummy.read_yaml_file("unexpected.yaml")

    dummy.lc.logger.error.assert_called_once()
    assert "Unexpected Exception" in dummy.lc.logger.error.call_args[0][0]
