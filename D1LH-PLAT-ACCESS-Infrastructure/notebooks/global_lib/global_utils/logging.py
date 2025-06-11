"""
Module: Logging Utility
Purpose: Provides a logging utility class (GlobalLogger) for capturing needed information and storing log messages as JSON files in a ADLS.
Version: 1.0.1
Author: Dmytro Ilienko
Owner: Dmytro Ilienko
Email: dmytro.ilienko.ext@bayer.com
Dependencies:
    - Python Standard Library: os, json, traceback, datetime
    - Databricks Utilities: dbutils, spark (available in the Databricks runtime environment)
Usage:
    from ...logging import GlobalLogger

    # Initialize the logger
    logger = GlobalLogger()

    # Log a simple informational message with additional context
    logger.log("This is a test message", custom_input={"object_name": "test_log"})

    # For asynchronous tasks, log errors from a Future object:
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        future = executor.submit(faulty_function)
        logger.log_async(future, custom_input={"object_name": "async_error"})
Reviewers: None
History:
    Date: 2025-03-01, Version: 1.0, Author: Dmytro Ilienko, Description: Creation of the script
    Date: 2025-05-27, Version: 1.0.1, Author: Martin Kopecky, Description: The _generate_extraction_path function now uses current logging context as default, making the log reading context-aware
"""
import json, os
import traceback
from datetime import datetime
from concurrent.futures import Future
from databricks.sdk.runtime import *

class GlobalLogger():
  def __init__(self):
    """
    Initialize the global_logging object.

    This constructor initializes the logging instance by capturing the runtime environment,
    the current timestamp, and the execution context. It then extracts the notebook name from
    the context, sets up the logging root URL based on the environment, and computes the logging path.
    """
    self.env = os.environ.get("env")
    self.current_datetime = datetime.now()
    self.context = self._receive_context()
    self.notebook_name = self.context['notebook_path'].split('/')[-1]
    self.logging_root = f"abfss://extlog-db@stelsalogs{self.env}.dfs.core.windows.net"
    self.logging_path = self._receive_logging_folder()

  def _receive_logging_folder(self):
    """
    Internal: Determine the logging folder path.

    Constructs the correct folder path for log storage based on the current execution context.
    If a job name is present in the context, logs are stored under a 'jobs' subfolder; otherwise,
    they are stored under a 'notebooks' subfolder using the notebook name.

    Returns:
        str: The complete directory path where log files will be saved.
    """
    if self.context['job_name']:
        folder = f"jobs/{self.context['job_name']}"
    else:
        folder = f"notebooks/{self.notebook_name}"
    path = f"{self.logging_root}/{folder}"
    return path

  def _generate_log_message(self, log_object, custom_input: str):
    """
    Internal: Build a structured log message.

    Creates a log message dictionary containing a timestamp, provided custom input,
    the execution context, and details about the log event. If the log_object is an exception,
    additional error information such as the exception type, stack trace, and any Spark-specific error
    details are included; otherwise, the log_object is simply converted to a string message.

    Parameters:
        log_object (Any): The object or exception to log.
        custom_input (str): Additional context to include in the log message.

    Returns:
        dict: A dictionary representing the complete log message.
    """
    if isinstance(log_object, Exception):
      log_message_severity = "ERROR"
      if hasattr(log_object, "getErrorClass"):
        spark_error_info = {
            "error_class": log_object.getErrorClass(),
            "sql_state": log_object.getSqlState(),
            "message_parameters": str(log_object.getMessageParameters())
        }
      else:
        spark_error_info = {}

      log_details = {
        "exception_type": type(log_object).__name__,
        "spark_error_info": spark_error_info,
        "stack_trace": traceback.format_tb(log_object.__traceback__),
        "log_message": str(log_object)
        }
    else:
      log_message_severity = "INFO"
      log_details = {"log_message": str(log_object)}

    # Create message dictionary
    message = {
      "timestamp": self.current_datetime.isoformat(),
      "input": custom_input,
      "execution_context": self.context,
      "log_message_severity": log_message_severity,
      "logger": "GlobalLogger",
      "log_details": log_details
    }
    return message

  def _receive_context(self):
    """
    Internal: Retrieve the execution context from Databricks.

    Attempts to obtain a comprehensive execution context using Databricks' dbutils.
    If that fails (for example, in a shared mode cluster), it falls back to collecting
    basic notebook information. The final context includes environment details, workspace
    and cluster identifiers, job and task information, as well as notebook metadata.

    Returns:
        dict: A dictionary containing the execution context data.
    """
    try:
        context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
        # manually adding the workspace name
        context['tags']['workspace_name'] = spark.table("generaldiscovery_observability.workspaces_metadata").filter(f"workspaceid='{context['tags'].get('orgId')}'").select("WorkspaceName").collect()[0][0]
    except Exception as e:
        print("Full context not available on shared mode cluster, providing only notebook information.")
        context = {
          'tags': {'user': spark.sql("select current_user()").collect()[0][0]},
          'extraContext': {'notebook_path': dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
                           'notebook_id': dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookId().get()}
          }

    context = {
        "environment": self.env,
        "platform": "databricks",
        "workspace_id": context['tags'].get('orgId'),
        "workspace_name": context['tags'].get('workspace_name'),
        "cluster_id": context['tags'].get('clusterId'),
        "job_id": context['tags'].get('jobId'),
        "job_name": context['tags'].get('jobName'),
        "job_run_id": context['tags'].get('jobRunId'),
        "task_run_id": context['tags'].get('runId'),
        "task_name": context['tags'].get('taskKey'),
        "notebook_id": context['extraContext'].get('notebook_id'),
        "notebook_path": context['extraContext'].get('notebook_path'),
        "user": context['tags'].get('user')
    }
    return context

  def log(self, log_object, custom_input: dict = {}):
    """
    Log an event or error by writing a formatted JSON log file to the designated logging path.

    This method captures the current datetime, generates a log message using the provided log object and custom input,
    and serializes the message into a JSON string. A unique log file name is then constructed using the custom input's
    "object_name" key (defaulting to "log" if not provided), the job run ID from the context, and a timestamp.
    The log file is written to a date-based folder on the ADLS storage, and a confirmation message with the 
    log file location is printed.

    Parameters:
        log_object (Any): The object to log. This may be an Exception (for error logging) or any custom log message.
        custom_input (dict, optional): A dictionary containing additional context or parameters to include in the log message. For example, it can contain an "object_name" key used in naming the log file. Defaults to an empty dictionary.

    Usage Example:
        # Log a custom message with additional context
        logger.log("Test message", custom_input={"object_name": "P2R_BSEG"})
        # Log an exception with additional context
        try:
            1 / 0
        except Exception as e:
            logger.log(e)
    """
    self.current_datetime = datetime.now()
    message = self._generate_log_message(log_object, custom_input)
    message_str = json.dumps(message, indent=4)
    timestamp = self.current_datetime.strftime('%Y%m%d%H%M%S%f')
    current_date = self.current_datetime.strftime("%Y-%m-%d")

    log_file_name = f"{custom_input.get('object_name', 'log')}-{self.context.get('job_run_id')}-{timestamp}.json"
    logging_path = f"{self.logging_path}/{current_date}/{log_file_name}"
    dbutils.fs.put(logging_path, message_str, overwrite=True)
    print(f"Logged error to {logging_path}")

  def log_async(self, future: Future, custom_input: dict = {}):
    """
    Asynchronously log an error by checking a Future for exceptions and delegating to the synchronous log method if necessary.

    This method inspects the provided Future to determine if it encountered an exception by calling its .exception() method.
    If an exception is present, it calls the synchronous log() method to log the error along with any provided custom input.
    This approach is useful for capturing errors from asynchronous or parallel operations.

    Parameters:
        future (Future): A Future object representing an asynchronous computation. The Future must support the .exception() method.
        custom_input (dict, optional): A dictionary containing additional context to include in the log message.

    Usage Example:
        from concurrent.futures import ThreadPoolExecutor
        def faulty_function():
            raise ValueError("An error occurred in the async task")
        with ThreadPoolExecutor() as executor:
            future = executor.submit(faulty_function)
            logger.log_async(future, custom_input={"object_name": "async_error"})
    """
    exc = future.exception()
    if exc:
        self.log(exc, custom_input)

  def _generate_extraction_path(self, notebook_name: str = None, job_name: str = None, date: str = None, file_index: str = None):
    """
    Internal: Generate the extraction path for logs based on provided parameters and instance attributes.

    This internal helper method constructs the file system path where logs are stored.
    It uses the base logging root and appends the appropriate subfolder for jobs or notebooks.
    The date (formatted as "YYYY-MM-DD") is appended unless set to "*". If a file index is provided,
    the method retrieves the specific file path from the directory listing. If no parameters are provided, 
    the extraction path defaults to the current logging context of the instance.

    It also verifies that the constructed path exists by attempting to list its contents.
    If the path does not exist, an Exception is raised.

    Parameters:
        notebook_name (str, optional): The name of the notebook to target. If not provided, the instance's default is used.
        job_name (str, optional): The job name to target. Takes precedence over notebook_name if provided.
        date (str, optional): The date folder to append to the path (format: "YYYY-MM-DD"). 
                              Defaults to the current date if not provided; set to "*" to include all date folders.
        file_index (int, optional): The index of the file in the directory listing to select.
                                    If provided, returns the path of the file at that index.

    Returns:
        str: The full path to the logs folder or specific log file if file_index is given.

    Raises:
        Exception: If the constructed log path does not exist.
    """

    if job_name:
      path = f"{self.logging_root}/jobs/{job_name}"
    elif notebook_name:
      path = f"{self.logging_root}/notebooks/{notebook_name}"
    else:
      path = self.logging_path

    if date is None:
      date = self.current_datetime.strftime("%Y-%m-%d")
    path = f"{path}/{date}"

    if file_index:
      path = dbutils.fs.ls(path)[file_index].path

    return path

  def get_log_df(self, notebook_name: str = None, job_name: str = None, date: str = None):
    """
    Retrieve logs as a Spark DataFrame.

    This method reads all JSON files found in the /notebooks or /jobs directory (and its
    subdirectories) using Spark's JSON reader with recursive file lookup enabled.
    "*" can be used in any parameter to select all notebooks/jobs/dates.
    If no parameters are provided, the logs for the logging context of the current instance 
    for today's date are extracted.

    Usage Example:
        # Retrieve logs for a specific notebook on a given date
        df = logger.get_log_df(notebook_name="MyNotebook", date="2023-03-25")
        # Retrieve logs for a specific job (job_name takes precedence over notebook_name) for the current date
        df = logger.get_log_df(job_name="MyJob")
        # Retrieve logs for all available dates for the current notebook
        df = logger.get_log_df(date="*)
        # Retrieve logs for all jobs for all dates
        df = logger.get_log_df(job_name="*", date="*")

    Parameters:
        notebook_name (str, optional): The notebook name to target. Defaults to None.
        job_name (str, optional): The job name to target. Defaults to None.
        date (str, optional): The date folder (format: "YYYY-MM-DD") where logs are stored. Defaults to None.

    Returns:
        DataFrame: A Spark DataFrame containing the log entries.
    """
    path = self._generate_extraction_path(notebook_name, job_name, date)
    df = (
      spark.read
      .option("recursiveFileLookup", "true")
      .option("multiLine", "true")
      .json(path)
    )
    return df

  def get_log(self, notebook_name: str = None, job_name: str = None, date: str = None, file_index: int = -1):
    """
    Retrieve a specific log file as a JSON/Python dictionary object.

    This method determines the full path to a log file using `_generate_extraction_path` with an optional
    file index (default is -1 which is the last file). It then copies the log file from the distributed 
    file system to the local disk, reads the file into a Python dictionary, and cleans up the local copy.
    "*" is not supported in any parameter. If no parameters are provided, the last log file for the logging 
    context of the current instance for today's date are extracted.

    Usage Example:
        # Retrieve the last log entry for a specific notebook on a given date
        log_entry = logger.get_log(notebook_name="MyNotebook", date="2023-03-25")
        # Retrieve the log entry at index 0 for a specific job on a give date
        log_entry = logger.get_log(job_name="MyJob", date="2023-03-25", file_index=0)

    Parameters:
        notebook_name (str, optional): The notebook name to target. Defaults to None.
        job_name (str, optional): The job name to target. Defaults to None.
        date (str, optional): The date folder (format: "YYYY-MM-DD") where the log is stored. Defaults to None.
        file_index (int, optional): The index of the file to retrieve within the folder. Defaults to -1 (typically the last file).

    Returns:
        dict: The log file content parsed as a JSON/dictionary object.
    """
    if "*" in (notebook_name, job_name, date, file_index):
      raise Exception("Invalid input: date, notebook_name, job_name, or file_index cannot be '*'")
    path = self._generate_extraction_path(notebook_name, job_name, date, file_index)
    # Extract the file name from the path
    file_name = path.split('/')[-1]
    # Copy the file to the local disk
    dbutils.fs.cp(path, f"file:///local_disk0/{file_name}")
    # Read the file
    with open(f"/local_disk0/{file_name}", "r") as f:
        file_content = json.load(f)
        f.close()
    # Delete the file from the local disk
    dbutils.fs.rm(f"file:///local_disk0/{file_name}", True)

    return file_content