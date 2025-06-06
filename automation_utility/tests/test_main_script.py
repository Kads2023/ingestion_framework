import pytest
import builtins
import yaml
import argparse


@pytest.fixture
def dummy_config():
    return {
        "schema_file_location": "some/path",
        "schema_file_name": "schema.json",
        "template_location": "some/templates",
        "output_location": "out",
        "data_source_system_name": "ERP",
        "source_system_type": "oracle",
        "db_user": "user",
        "dsn": "dsn",
        "schema_name": "schema"
    }


def test_main(monkeypatch, tmp_path, dummy_config):
    # 1. Fake CLI arguments
    monkeypatch.setattr(
        "argparse.ArgumentParser.parse_args",
        lambda self: argparse.Namespace(tables="EMPLOYEES,DEPT", config="dummy.yaml")
    )

    # 2. Fake open(config file)
    dummy_yaml_str = yaml.dump(dummy_config)

    class DummyFile:
        def __init__(self, content):
            self.content = content
            self._lines = content.splitlines()
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def read(self): return self.content
        def readline(self): return self._lines.pop(0)

    monkeypatch.setattr(builtins, "open", lambda path, mode="r": DummyFile(dummy_yaml_str))

    # 3. Monkeypatch yaml.safe_load
    monkeypatch.setattr("yaml.safe_load", lambda f: dummy_config)

    # 4. Monkeypatch generate_artifacts
    called = {}

    def mock_generate_artifacts(config):
        called["generate_artifacts"] = True
        assert config["source_system_type"] == "oracle"

    monkeypatch.setattr("edp_automation.utils.generate_artifacts_from_schema.generate_artifacts", mock_generate_artifacts)

    # 5. Optional: monkeypatch generate_schema_file if it's uncommented later
    monkeypatch.setattr("edp_automation.utils.generate_schema_file_from_source.generate_schema_file", lambda *args, **kwargs: None)

    # 6. Run main()
    from edp_automation.utils.generate_artifacts import main
    main()

    assert called.get("generate_artifacts") is True


def test_main_invalid_config(monkeypatch):
    monkeypatch.setattr(
        "argparse.ArgumentParser.parse_args",
        lambda self: argparse.Namespace(tables="TAB1", config="missing.yaml")
    )

    monkeypatch.setattr(builtins, "open", lambda path, mode="r": (_ for _ in ()).throw(FileNotFoundError("file not found")))

    from edp_automation.utils.generate_artifacts import main

    with pytest.raises(FileNotFoundError):
        main()
