import yaml
from edp_automation.utils.generate_artifacts_from_schema import generate_artifacts

config_file = "C:/Users/srin8/PycharmProjects/ingestion_framework/automation_utility/src/edp_automation/resources/inputs/config.yml"

# Read config file
with open(config_file, "r") as config_data:
    config = yaml.safe_load(config_data)

generate_artifacts(config)