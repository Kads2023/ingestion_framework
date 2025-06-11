import pytest
import json
import os
from pathlib import Path
from edp_automation.utils.generate_artifacts_from_schema import generate_artifacts

from jinja2 import TemplateNotFound


class DummyMapper:
    def map_type(self, entry):
        return f"mapped_{entry['data_type'].lower()}"


@pytest.fixture
def sample_schema():
    return [
        {
            "table_name": "EMPLOYEES",
            "column_name": "ID",
            "data_type": "NUMBER",
            "nullable": "N",
            "identity_column": "YES",
            "comments": "Primary key"
        },
        {
            "table_name": "EMPLOYEES",
            "column_name": "NAME",
            "data_type": "VARCHAR2",
            "nullable": "Y",
            "identity_column": "NO",
            "comments": "Employee name"
        }
    ]


@pytest.fixture
def config_and_templates(tmp_path, sample_schema):
    # Create a dummy schema JSON file
    schema_file_path = tmp_path / "schema.json"
    schema_file_path.write_text(json.dumps(sample_schema, indent=2))

    # Create dummy Jinja2 templates
    template_dir = tmp_path / "templates"
    template_dir.mkdir()
    for tpl in ["ddl.sql.j2", "config.yaml.j2", "workflow.yaml.j2"]:
        (template_dir / tpl).write_text(
            "{{ table_name }}\n{% for col in columns %}{{ col.name }} {{ col.datatype }}{% endfor %}"
        )

    # Output directory
    output_dir = tmp_path / "output"

    config = {
        "schema_file_location": str(tmp_path),
        "schema_file_name": "schema.json",
        "template_location": str(template_dir),
        "ddl_template_name": "ddl.sql.j2",
        "config_template_name": "config.yaml.j2",
        "workflow_template_name": "workflow.yaml.j2",
        "output_location": str(output_dir),
        "data_source_system_name": "ERP",
        "source_system_type": "oracle"
    }

    return config, output_dir


def test_generate_artifacts_success(monkeypatch, config_and_templates):
    config, output_dir = config_and_templates

    printed = []
    monkeypatch.setattr("builtins.print", lambda x: printed.append(x))
    monkeypatch.setattr(
        "edp_automation.utils.generate_config_and_ddl_from_schema.DataTypeMapperFactory.get_mapper",
        lambda source_system_type: DummyMapper()
    )

    generate_artifacts(config)

    emp_dir = output_dir / "employees"
    assert (emp_dir / "EMPLOYEES.sql").exists()
    assert (emp_dir / "employees.yaml").exists()
    assert (emp_dir / "edp_bronze_erp_employees.yaml").exists()

    contents = (emp_dir / "EMPLOYEES.sql").read_text()
    assert "EMPLOYEES" in contents
    assert "ID mapped_number" in contents
    assert "NAME mapped_varchar2" in contents

    assert any("Generated:" in msg for msg in printed)


def test_generate_artifacts_missing_schema(monkeypatch, tmp_path):
    config = {
        "schema_file_location": str(tmp_path),
        "schema_file_name": "missing.json",
        "template_location": str(tmp_path),
        "ddl_template_name": "ddl.sql.j2",
        "config_template_name": "config.yaml.j2",
        "workflow_template_name": "workflow.yaml.j2",
        "output_location": str(tmp_path / "output"),
        "data_source_system_name": "ERP",
        "source_system_type": "oracle"
    }

    with pytest.raises(FileNotFoundError):
        generate_artifacts(config)




@pytest.fixture
def sample_schema_new(tmp_path):
    schema_data = [
        {
            "table_name": "EMP",
            "column_name": "ID",
            "data_type": "NUMBER",
            "nullable": "N",
            "identity_column": "YES",
            "comments": "Employee ID"
        },
        {
            "table_name": "EMP",
            "column_name": "NAME",
            "data_type": "VARCHAR2",
            "nullable": "Y",
            "identity_column": "NO",
            "comments": "Employee Name"
        }
    ]
    schema_file = tmp_path / "schema.json"
    with open(schema_file, "w") as f:
        json.dump(schema_data, f)
    return schema_file


@pytest.fixture
def sample_config(tmp_path, sample_schema_new):
    return {
        "schema_file_location": str(sample_schema_new.parent),
        "schema_file_name": sample_schema_new.name,
        "template_location": None,  # Use internal templates
        "output_location": str(tmp_path / "output"),
        "source_system_type": "oracle",
        "data_source_system_name": "hr",
        "artifacts_to_generate": "all",
        "ddl_template_name": "ddl.sql.j2",
        "ddl_file_name": "create_{table_name}.sql",
        "config_template_name": "config.yaml.j2",
        "config_file_name": "config_{data_source_system_name}_{table_name}.yaml",
        "workflow_template_name": "workflow.yaml.j2",
        "extra_templates": [
            {
                "template_name": "config.yaml.j2",
                "output_file_name": "{table_name}_{data_source_system_name}_extra.yaml"
            }
        ]
    }


def test_generate_artifacts_success_new(sample_schema_new, monkeypatch, tmp_path):
    # Mock DataTypeMapper
    class DummyMapper:
        def map_type(self, entry):
            return f"{entry['data_type'].lower()}_mapped"

    monkeypatch.setattr(
        "edp_automation.data_type_mapper.data_type_mapper_factory.DataTypeMapperFactory.get_mapper",
        lambda source: DummyMapper()
    )

    # Mock internal template loader
    class DummyTemplate:
        def __init__(self, name):
            self.name = name

        def render(self, context):
            return f"{self.name}_rendered_for_{context['table_name']}"

    class DummyEnv:
        def get_template(self, name):
            if "missing" in name:
                raise TemplateNotFound(name)
            return DummyTemplate(name)

    monkeypatch.setattr(
        "edp_automation.utils.generate_config_and_ddl_from_schema.Environment",
        lambda loader, **kwargs: DummyEnv()
    )

    generate_artifacts(sample_config)

    emp_dir = tmp_path / "output" / "emp"
    assert (emp_dir / "create_EMP.sql").exists()
    assert (emp_dir / "config_hr_emp.yaml").exists()
    assert (emp_dir / "emp_hr_extra.yaml").exists()


def test_generate_artifacts_missing_schema_file(monkeypatch):
    config = {
        "schema_file_location": "/invalid/path",
        "schema_file_name": "missing.json",
        "output_location": ".",
        "source_system_type": "oracle",
        "data_source_system_name": "hr"
    }

    with pytest.raises(FileNotFoundError):
        generate_artifacts(config)


def test_generate_artifacts_custom_template_dir(sample_config, monkeypatch, tmp_path):
    # Set a dummy template dir
    template_dir = tmp_path / "templates"
    template_dir.mkdir()
    (template_dir / "ddl.sql.j2").write_text("create ddl for {{ table_name }}")

    sample_config["template_location"] = str(template_dir)
    sample_config["extra_templates"] = []

    class DummyMapper:
        def map_type(self, entry):
            return entry["data_type"]

    monkeypatch.setattr(
        "edp_automation.data_type_mapper.data_type_mapper_factory.DataTypeMapperFactory.get_mapper",
        lambda source: DummyMapper()
    )

    generate_artifacts(sample_config)

    emp_dir = tmp_path / "output" / "emp"
    assert (emp_dir / "create_EMP.sql").exists()
    content = (emp_dir / "create_EMP.sql").read_text()
    assert "create ddl for EMP" in content


def test_generate_artifacts_invalid_template(monkeypatch, sample_config):
    sample_config["ddl_template_name"] = "missing_template.sql.j2"

    class DummyMapper:
        def map_type(self, entry):
            return entry["data_type"]

    monkeypatch.setattr(
        "edp_automation.data_type_mapper.data_type_mapper_factory.DataTypeMapperFactory.get_mapper",
        lambda source: DummyMapper()
    )

    # Patch environment to raise TemplateNotFound
    class DummyEnv:
        def get_template(self, name):
            raise TemplateNotFound(name)

    monkeypatch.setattr(
        "edp_automation.utils.generate_config_and_ddl_from_schema.Environment",
        lambda loader, **kwargs: DummyEnv()
    )

    with pytest.raises(TemplateNotFound):
        generate_artifacts(sample_config)


def test_generate_artifacts_with_single_artifact_type(monkeypatch, sample_config, tmp_path):
    sample_config["artifacts_to_generate"] = "ddl"
    sample_config["extra_templates"] = []

    class DummyMapper:
        def map_type(self, entry):
            return entry["data_type"]

    monkeypatch.setattr(
        "edp_automation.data_type_mapper.data_type_mapper_factory.DataTypeMapperFactory.get_mapper",
        lambda source: DummyMapper()
    )

    class DummyTemplate:
        def render(self, context):
            return "dummy"

    class DummyEnv:
        def get_template(self, name):
            return DummyTemplate()

    monkeypatch.setattr(
        "edp_automation.utils.generate_config_and_ddl_from_schema.Environment",
        lambda loader, **kwargs: DummyEnv()
    )

    generate_artifacts(sample_config)

    emp_dir = tmp_path / "output" / "emp"
    assert (emp_dir / "create_EMP.sql").exists()
    assert not (emp_dir / "config_hr_emp.yaml").exists()
