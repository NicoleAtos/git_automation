from pyspark.dbutils import DBUtils 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
import json
from datetime import datetime
import global_utils.exceptions as exception_utility

import json
class global_loging():
  def __init__(self):
    context = self._receive_context()
    self.job_run_id = self.context.get("JobRunId")
    self.job_name = self.context.get("JobName")
    self.task_name = self.context.get("TaskName")
    self.logging_base_path = self._receive_logging_folder()
    self.contex = ""
    self.static_information = ""
    self.valid_inputs = {"log_message_severity" : {"valid_data_range" : ["INFO","WARN","ERROR","DEBUG"]}}
  
  def get_logs(self):
    """
    Display the latest logs as dataframe.
    
    Returns:
      dataFrame: Last 10 logs written to the logging folder
    """
    return(spark.
             read.
             option("recursiveFileLookup","true").
             json(self._receive_logging_folder()).
             orderBy(F.col("JOB_RUN_DATE").
             desc()).
             limit(10))
  def _receive_logging_folder(self):
    env = spark.conf.get("spark.hadoop.javax.jdo.option.ConnectionURL")[-3:]
    path =  f"abfss://extlog@stelsalogs{env}.dfs.core.windows.net/"
    
    # Test if logging path exist
    try:
      dbutils.fs.ls(path)
    except Exception:
      raise log_error("pipeline can not access the specified log folder.")
    return path
  """
  Set Information to appear in every log message. The expected input is a dictionary containing the following structure:
  {"InformationKey1: InformationValue1,
    InformationKey2: InformationValue2"}

  """
  
  def set_constant_attributes(self, attributes : dict):
    """
      
    """
    string = []
    # Control that type is attribute
    for key in attributes:
      string.append(f'"{key}" : "{attributes[key]}"')
    self.static_information += ", ".join(string)
    
  def _validate_inputs(self, Inputs):
    
    # Validate the input information
    for key in Inputs:
      validation_rule = self.valid_inputs.get(key)
      if validation_rule is not None:
        valid_data_range = validation_rule.get("valid_data_range")
        
        
      
  def message(self, log_message: str, table_name : list = [], log_message_severity: str = "INFO", additional_info: str = ""):
    # Validate the Input values
    self._validate_inputs({"LOG_MESSAGE_SEVERITY":log_message_severity})
    timestamp = datetime.now()
    
    # Create message
    message = {"JOB_RUN_ID" : self.job_run_id,
               "JOB_RUN_DATE" : timestamp.strftime("%Y-%m-%d %H:%M:%S.%f"),
               "SOURCE" : "Databricks",
               "JOB_NAME" : self.job_name,
               "TASK_NAME" : self.task_name,
               "TABLE_NAME" : table_name,
               "LOG_MESSAGE" : log_message,
               "LOG_MESSAGE_SEVERITY" : log_message_severity,
               "ADDITIONAL_INFO" : f"{self.static_information} {additional_info}"
              }
    
    message_str = json.dumps(message)
    message_file_name = f"{self.job_run_id}_{timestamp.strftime('%Y%m%d%H%M%S%f')}.json"
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # Write message to json file
    logging_folder = f"{self.logging_base_path}{current_date}/{message_file_name}"
    dbutils.fs.put(logging_folder, message_str, True)
  
  def _receive_context(self):  
    # receive context from dbutils
    context = {"tags":{},"extraContext":{}}
    # check if dependency packages do exist
    if context.get("currentRunId") is None:
      self.context = {"JobName" : "MANUAL",
                     "JobRunId" : f"{context.get('tags').get('commandRunId')}",
                     "TaskName" : context.get("extraContext").get("notebook_path")}
    else:
      self.context = {"JobName" : f"{context.get('tags').get('jobName')}",
                     "JobRunId" : f"{context.get('tags').get('multitaskParentRunId')}",
                     "TaskName" : context.get("tags").get("taskKey")}

