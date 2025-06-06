import pytest
import json
import os
from pathlib import Path
from edp_automation.utils.generate_artifacts_from_schema import generate_artifacts


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
