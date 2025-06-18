"""
Module: Logging Utility
Purpose: Provides a logging utility class (GlobalLogger) for capturing needed information and storing log messages as JSON files in a ADLS.
Version: 1.1.0
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

    # Log a warning message
    logger.log("This is a warning message", custom_input={"object_name": "test_log"}, severity="WARNING")

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
import warnings
from datetime import datetime
from concurrent.futures import Future
from databricks.sdk.runtime import *

class GlobalLogger():
    """
    Purpose: Provides a unified logging utility class for capturing logs as JSON files in ADLS.
             Supports INFO, ERROR, and WARNING levels automatically.
             Python warnings are automatically intercepted and logged.
    """

    def __init__(self):
        """
        Initialize the GlobalLogger object.

        - Captures Databricks execution context.
        - Sets up environment, logging root and path.
        - Registers automatic warning hook to capture Python warnings.
        """
        self.env = os.environ.get("env")
        self.current_datetime = datetime.now()
        self.context = self._receive_context()
        self.notebook_name = self.context['notebook_path'].split('/')[-1]
        self.logging_root = f"abfss://extlog-db@stelsalogs{self.env}.dfs.core.windows.net"
        self.logging_path = self._receive_logging_folder()

        self._register_warning_hook()

    def _register_warning_hook(self):
        """
        Internal: Register Python warnings hook to automatically log runtime warnings.
        """
        def warning_handler(message, category, filename, lineno, file=None, line=None):
            warning_msg = f"{category.__name__}: {message} (in {filename}:{lineno})"
            message_dict = self._generate_log_message(warning_msg, {}, severity="WARNING")
            self._write_log_message(message_dict)

        warnings.showwarning = warning_handler

    def _receive_logging_folder(self):
        """
        Internal: Determine the logging folder path depending on context.

        Returns:
            str: The full directory path where log files will be saved.
        """
        if self.context['job_name']:
            folder = f"jobs/{self.context['job_name']}"
        else:
            folder = f"notebooks/{self.notebook_name}"

        return f"{self.logging_root}/{folder}"

    def _receive_context(self):
        """
        Internal: Retrieve the Databricks execution context.

        Returns:
            dict: Dictionary containing full execution metadata.
        """
        try:
            context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
            context['tags']['workspace_name'] = spark.table("generaldiscovery_observability.workspaces_metadata")\
                .filter(f"workspaceid='{context['tags'].get('orgId')}'")\
                .select("WorkspaceName").collect()[0][0]
        except Exception:
            print("Full context not available on shared mode cluster, falling back to notebook context only.")
            context = {
                'tags': {'user': spark.sql("select current_user()").collect()[0][0]},
                'extraContext': {
                    'notebook_path': dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
                    'notebook_id': dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookId().get()
                }
            }

        return {
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

    def _generate_log_message(self, log_object, custom_input: dict, severity: str = None):
        """
        Internal: Build structured log message for INFO, ERROR, and WARNING.

        Parameters:
            log_object (Any): The object or exception or warning string to log.
            custom_input (dict): Additional input context.
            severity (str, optional): Log severity override (only used for WARNING from hook).

        Returns:
            dict: Complete log message dictionary.
        """
        if severity == "WARNING":
            log_message_severity = "WARNING"
            log_details = {"log_message": str(log_object)}
        elif isinstance(log_object, Exception):
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

        return {
            "timestamp": self.current_datetime.isoformat(),
            "input": custom_input,
            "execution_context": self.context,
            "log_message_severity": log_message_severity,
            "logger": "GlobalLogger",
            "log_details": log_details
        }

    def _write_log_message(self, message):
        """
        Internal: Write the generated message to storage.
        """
        message_str = json.dumps(message, indent=4)
        timestamp = self.current_datetime.strftime('%Y%m%d%H%M%S%f')
        current_date = self.current_datetime.strftime("%Y-%m-%d")

        log_file_name = f"log-{self.context.get('job_run_id')}-{timestamp}.json"
        logging_path = f"{self.logging_path}/{current_date}/{log_file_name}"
        dbutils.fs.put(logging_path, message_str, overwrite=True)
        print(f"Logged {message['log_message_severity']} to {logging_path}")

    def log(self, log_object, custom_input: dict = {}):
        """
        Log an event or error by writing a formatted JSON log file to storage.

        Captures:
            - Exceptions as ERROR
            - All other objects as INFO

        Parameters:
            log_object (Any): Object or exception to log.
            custom_input (dict): Optional additional metadata.
        """
        self.current_datetime = datetime.now()
        message = self._generate_log_message(log_object, custom_input)
        self._write_log_message(message)
