ProcessMonitoring
The ProcessMonitoring class provides utilities to monitor and manage ETL jobs in a PySpark + Azure environment, including logging job metadata and execution statuses to an Azure SQL database.

📦 Dependencies
pyodbc: For connecting to Azure SQL Database.

struct: For formatting token bytes for Azure AD authentication.

pandas: For handling query results before converting to Spark DataFrame.

pyspark.sql: For creating Spark DataFrames.

azure.identity.DefaultAzureCredential: For Azure token-based authentication.

🔧 Initialization
python
Copy
Edit
ProcessMonitoring(lc, common_utils, job_args)
Parameters:
lc: Logging context object (should contain a logger).

common_utils: Utility class with helper methods like validate_function_param, get_current_time.

job_args: Object that supports .get() and .set() for configuration and state management (e.g., job IDs, table names).

🧠 Core Methods
get_conn()
Establishes a secure connection to an Azure SQL database using an access token from DefaultAzureCredential.

execute_query_and_get_results(passed_query, fetch_results=True)
Executes the given SQL query on the database and optionally returns results as a Spark DataFrame.

passed_query (str): SQL query to execute.

fetch_results (bool): If True, fetch query results. If False, run as a non-query (e.g., INSERT/UPDATE).

get_and_set_job_id(raise_exception=False)
Fetches and sets the job ID from the job_details table based on job arguments like source system, source type, etc. Optionally raises an exception if not found.

insert_job_details()
Inserts a new record into the job_details table based on the current job metadata.

check_and_get_job_id()
Checks if a job ID exists. If not, inserts a new job entry and fetches the ID.

check_already_processed(passed_job_id="", passed_run_date="")
Checks if the job for the given ID and run date has already been marked as COMPLETED.

get_and_set_run_id()
Fetches the run_id from the job_run_details table based on job_id, run_date, and run_start_time.

insert_update_job_run_status(passed_status, passed_comments="")
Inserts or updates the run status of a job in job_run_details. If run_id exists, it updates; otherwise, it inserts a new run record.

📁 Sample Usage
python
Copy
Edit
pm = ProcessMonitoring(lc, common_utils, job_args)

pm.check_and_get_job_id()
pm.check_already_processed()
pm.insert_update_job_run_status("RUNNING")

# Run your main logic

pm.insert_update_job_run_status("COMPLETED", "Job finished successfully.")
📝 Notes
Uses SparkSession from the current context.

Requires Azure AD access to fetch access tokens for DB auth.

Assumes job configuration and metadata are handled via job_args.
