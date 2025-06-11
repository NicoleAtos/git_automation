"""
Module: Helper Functions
Purpose: This module contains helper functions of general purpose.
Version: 1.0
Author: Dmytro Ilienko
Owner: Dmytro Ilienko
Email: dmytro.ilienko.ext@bayer.com
Dependencies: None
Usage: Every function should be called and pass the correct parameters.
Reviewers: None
History:
    Date: 2025-02-10, Version: 1.0, Author: Dmytro Ilienko, Description: Creation of the script
"""

from .global_functions import base_path, check_table
from pyspark.sql.dataframe import DataFrame
import azure.cosmos.cosmos_client as cosmos_client
from datetime import datetime,timedelta
import os, requests, json
import time
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from databricks.sdk.runtime import *
from pyspark.sql.utils import StreamingQueryException
from pyspark.sql.streaming import StreamingQuery

cosmos_endpoint = dbutils.secrets.get(scope="elsa-scope", key="cosmosDB-processing-metastore-endpoint")
cosmos_masterKey = dbutils.secrets.get(scope="elsa-scope", key="cosmosDB-processing-metastore-masterKey")
client = cosmos_client.CosmosClient(cosmos_endpoint, {'masterKey': cosmos_masterKey})
db_client = client.get_database_client("metadata")


def get_collection_from_cosmos(cosmos_container_name: str) -> list:
  """
  Function: get_collection_from_cosmos
  Description: Extracts a collection of entries from a specific container in CosmosDB.
  Parameters:
     - cosmos_container_name (str): Name of the container in CosmosDB.
  Returns: List - List of CosmosDB entries.
  Author: Dmytro Ilienko
  Date: 2023-01-01
"""
  container_client = db_client.get_container_client(cosmos_container_name)
  collection = [entry for entry in container_client.read_all_items()]
  return collection


def extract_list(input: str):
  if input=="*":
    return input
  else:
    output = input.replace(" ", "").split(",")
    output = output if any(output) else None
    return output


def get_primary_keys(source_system: str, table_name: str) -> list:
  """
    Function: get_primary_keys
    Description: Collects the primary key information for a specific table from the metadata table.
    Parameters:
      - source_system (str): Name of the table's source system.
      - table_name (str): Name of the table.
    Returns: List[str] - List of strings representing the primary key for a table.
    Author: Dmytro Ilienko
    Date: 2023-01-01
  """
  cosmos_entries = get_collection_from_cosmos("databricks-pipeline-metadata")
  metadata_list = [item for item in cosmos_entries if item["table_name"]==table_name or item["original_name"]==table_name]

  if metadata_list:
    primary_keys = metadata_list[0]["primary_key"]
  else:
    landing_base = base_path("landing", source_system)
    dd03l_keys = ['AS4LOCAL', 'AS4VERS', 'TABNAME', 'POSITION', 'FIELDNAME']
    dd03l = deduplication_function(spark.table(f"staging.{source_system}_DD03L"), dd03l_keys)
    fieldnames = (
      dd03l
      .select("FIELDNAME")
      .where(F.col("KEYFLAG") == "X")
      .where(F.col("TABNAME") == table_name)
      .where(F.col("FIELDNAME") != ".INCLUDE")
      .where(F.col("FIELDNAME") != ".INCLU--AP")
      .where(F.col("OPTYPE") != "D")
    )
    primary_keys = list(set(primary_key.FIELDNAME for primary_key in fieldnames.collect()))

  if not primary_keys:
    raise Exception("Missing primary key!")

  return primary_keys


def error_logging(error: str, file_name: str, folder: str="Base"):
  """
    Function: error_logging
    Description: Logs the provided error for a specific table into the logging storage account in ADLS.
    Parameters:
      - error (str): String containing the error.
      - file_name (str): Name of the target file.
      - folder (str, optional): Name of the folder where the error should be logged.
    Returns: None
    Author: Dmytro Ilienko
    Date: 2023-01-01
  """
  path = f"{base_path('logging')}/{folder}/{file_name}.txt"
  dbutils.fs.put(path, str(error), True)



def exceptions_check(multithread_output: list, raise_flag: bool=False):
  """
  Function: exceptions_check
  Description: Checks for exceptions in the output of a multithreaded operation and optionally raises an error.
  Parameters:
     - multithread_output (list): List of thread results, where each result may contain an exception.
     - raise_flag (bool, optional): If True, raises an exception when any errors are found. Default is False.
  Returns: None
  Author: Dmytro Ilienko
  Date: 2023-01-01
  """
  exception_list = [str(res.exception()).split('\n')[0] for res in multithread_output if res.exception()]
  if exception_list:
    print("List of exceptions:")
    print(*exception_list, sep='\n\n')
    if raise_flag:
      raise Exception(f"Something went wrong. Please find below/above the list of exceptions.")


def authorizationOauth2_CDP():
  """
  Function: authorizationOauth2_CDP
  Description: Retrieves an OAuth2 access token from AWS API using client credentials.
  Parameters: None
  Returns: str - The OAuth2 access token.
  Author: Sharad Gangwar
  Date: 2023-01-01
  """
  # setting up configuration to get the token
  client_id = dbutils.secrets.get(scope = "elsa-scope", key = "invoke-aws-api-client-id")
  client_secret = dbutils.secrets.get(scope = "elsa-scope", key = "invoke-aws-api-client-secret")
  client_scope = dbutils.secrets.get(scope = "elsa-scope", key = "invoke-aws-api-scope")
  tokenurl = dbutils.secrets.get(scope = "elsa-scope", key = "invoke-aws-api-token-url")

  body = {
      "grant_type": "client_credentials",
      "client_id": client_id,
      "scope": client_scope,
      "client_secret": client_secret,
  }
  token_url = tokenurl
  tokenResponse = requests.post(token_url, data=body)
  token = tokenResponse.json()["access_token"]
  return token


def invoke_api(payload):
  """
  Function: invoke_api
  Description: Sends a POST request to the AWS API with the given payload and OAuth2 authentication.
  Parameters:
     - payload (dict or str): The request body to send to the API.
  Returns: Tuple[str, int] - The API response text and the HTTP status code.
  Author: Sharad Gangwar
  Date: 2023-01-01
  """
  url = dbutils.secrets.get(scope = "elsa-scope", key = "invoke-aws-api-url")
  token = authorizationOauth2_CDP()
  auth_header = {'Authorization': 'Bearer ' + token,
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
  response = requests.post(url, data=payload, headers=auth_header)
  return response.text, response.status_code


def table_load_notification(discovery_db: str, table_name: str, job_name: str, finished_time):
  """
  Function: table_load_notification
  Description: Logs table load details to the observability schema and notifies an API if the table exists in the discovery storage account.
  Parameters:
     - discovery_db (str): Name of the discovery database (schema).
     - table_name (str): Name of the table being logged.
     - job_name (str): Name of the job that processed the table.
     - finished_time (datetime): Timestamp indicating when the job finished.
  Returns: None
  Author: Sharad Gangwar
  Date: 2023-01-01
  """
  cwid_api = dbutils.secrets.get(scope = "elsa-scope", key = "invoke-aws-api-cwid")
  try:
      schema = T.StructType([
        T.StructField("Schema_name", T.StringType(), True),
        T.StructField("Table_name", T.StringType(), True),
        T.StructField("Job_name", T.StringType(), True),
        T.StructField("Finished_time", T.StringType(), True)
      ])
      log_data = [(discovery_db, table_name, job_name, finished_time)]
      log_df = spark.createDataFrame(log_data, schema)

      config_df = spark.read.table(
          "generaldiscovery_observability.job_group_table_config"
      ).select("table_name", "schema_name", "datasource", "payload")

      config_df_final = config_df.filter(
          (F.lower(F.col("schema_name")) == discovery_db.lower()) &
          (F.lower(F.col("table_name")) == table_name.lower())
      )

      if config_df_final.count() == 1:
          # Write log to the observability schema
          log_df.write.format("delta").option("mergeSchema", "true").mode(
              "append"
          ).saveAsTable("generaldiscovery_observability.job_table_log")
          row = config_df_final.head()
          payload_dict = json.loads(row["payload"])

          dict1 = {
              "type": "SAPElsa_LOAD_READY",
              "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
              "datasource": row["datasource"],
              "cwid": cwid_api,
              "payload": payload_dict,
              "dryrun": "false",
          }
          dict_json = json.dumps(dict1)
          # Invoke API
          response_text, status_code = invoke_api(dict_json)
          response_api_data = [(job_name, table_name, response_text, status_code, finished_time)]
          response_schema = T.StructType([
              T.StructField("job_name", T.StringType(), True),
              T.StructField("table_name", T.StringType(), True),
              T.StructField("response_text", T.StringType(), True),
              T.StructField("status_code", T.IntegerType(), True),
              T.StructField("finished_time", T.StringType(), True)
          ])
          response_df = spark.createDataFrame(response_api_data, response_schema)
          response_df.write.format("delta").option("mergeSchema", "true").mode(
              "append"
          ).saveAsTable("generaldiscovery_observability.table_api_response")
  except Exception as e:
      print(f"An error occurred: {e}")


def generate_stopping_time(hour: int, default: datetime) -> datetime:
  """
  Generates the datetime based on provided hour which is later used as a stopping time for the pipeline.

  Args:
      hour: Target hour for the datetime.
  
  Returns:
      Target time when the processing of the pipeline will be stopped.
  """
  if hour:
    hour = int(hour)
    stopping_time = datetime.now().replace(hour=hour, minute=0, second=0, microsecond=0)
    if hour <= datetime.now().hour:
      stopping_time = stopping_time + timedelta(days=1)
  else:
    stopping_time = default
  
  return stopping_time


def get_seconds_left(target_time: datetime) -> int:
  """
  Calculates the difference between provided target time and current time in seconds. Used to determine the timeout in seconds for streaming queries.

  Args:
      target_time: Target time when the processing of the pipeline should be stopped.
  
  Returns:
      Integer representing number of seconds left.
  """
  if target_time > datetime.now():
    seconds = (target_time - datetime.now()).total_seconds()
  else:
    seconds = None
    
  return seconds


def stopStream(stream: StreamingQuery):
  """
   Utility function to safely terminate the streaming query.
   It will finish processesing the current batch, then checks for new data at source.
   It will only stop the stream when no new data is available in source.
   This might be a problem when source is expected to get new data for a long time after calling this function, as the stream will not stop.
   Another caveat is that the stream might actually stop when --by pure chance-- there is no new data in source at the time of the check. So some batches might not be processed by today's job run, but only picked up by the next day's run.
   See the soft_stopStream() and hard_stopStream functions for different stream stopping scenarios.
  Args:
      stream: Streaming query object.
  """
  while stream.isActive:
    if not stream.status['isDataAvailable'] and stream.status['message'] != "Processing new data":
      try:
        print("Stopping the stream {}.".format(stream.name))
        stream.stop()
        print(f"The stream {stream.name} was stopped at {time.strftime('%Y-%m-%d %H:%M:%S')}")
      except:
        # In extreme cases, this funtion may throw an ignorable error.
        print("An [ignorable] error has occured while stoping the stream.")
    time.sleep(5)


def get_models_list(content, path, model_ids=None, deep_search=False, use_data_folder=False):
    """Recursively collects all model files from subfolders until the deepest level, while skipping files containing '_streaming_' unless in model_ids.

    Args:
        content (str): The content folder name (e.g., contract2cash).
        path (str): The path to search within, supporting bracketed subdirectories.
        model_ids (list): List of model IDs to search for (optional).
        deep_search (bool): Whether to search deeper within a folder once files are found.
        use_data_folder (bool): If True, search in the 'data' folder instead of 'models'.

    Returns:
        list: A list of file paths found within all nested subfolders.
    """
    folder_type = path if path == "data" else "models"
    models_folder = f"../../../{content.lower()}/{folder_type}"
    models_list = []
    
    # Ensure model_ids is a list
    if isinstance(model_ids, str):
        model_ids = [model_ids]

    # Split the path if it contains brackets for multiple directories
    if "[" in path and "]" in path:
        bracket_part = path.split("[")[-1].split("]")[0]
        subfolders = bracket_part.split(",")
        base_path = path.split("]")[-1].strip("/")
        path_list = [f"{subfolder}/{base_path}" for subfolder in subfolders]
    else:
        path_list = [""] if path == "data" else [path]

    def search_files(folder, is_deep):
        """Recursively searches for files in all subdirectories."""
        if not os.path.exists(folder):
            return

        for entry in os.scandir(folder):
            if entry.is_file():
                if "Archive" not in entry.path.split(os.sep):  # Excluding 'Archive' folders from all directories
                    models_list.append(entry.path)
            elif entry.is_dir() and is_deep:
                search_files(entry.path, is_deep)  # Recursive call for deeper directories

    for subfolder in path_list:
        subfolder_path = os.path.join(models_folder, subfolder)
        search_files(subfolder_path, deep_search)

    if model_ids:
        models_list = [
            path for path in models_list # iterate through all available model paths
            if os.path.basename(path) in model_ids # check if the file name is in the input list of model IDs
            or os.path.splitext(os.path.basename(path))[0] in {
                os.path.splitext(f)[0] for f in model_ids 
                if '.' not in f
            } # check if the file name (without extension) is in the input list of model IDs
        ]
        if len(models_list) != len(model_ids):
            raise ValueError("Not all model IDs were found in the specified path.")
    else:
        models_list = [path for path in models_list if "_streaming_" not in os.path.basename(path)]
    
    return models_list
