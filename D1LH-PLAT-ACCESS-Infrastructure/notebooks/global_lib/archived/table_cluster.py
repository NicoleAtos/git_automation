import globalUtilities.receive_metadata as metadata_utility
import globalUtilities.log as log_utility 
import globalUtilities.exceptions as exception_utility
from pyspark.dbutils import DBUtils 
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

class cluster_assignement():
  def __init__(self):
    self.log = log_utility.global_loging()
    self.meta_data = metadata_utility.meta_data_generation()
  
  def _get_cluster_meta_data(self, selected_cluster : list):
    # Implement Interface to MetaData Classes
    cluster_assignment = self.meta_data.get_cluster()
    selected_cluster = [entry.upper() for entry in selected_cluster]
    print(selected_cluster)
    cluster = [entry for entry in cluster_assignment if entry.get("id").upper() in selected_cluster]
    # check if entry exist in cluster assignment metadata
    if cluster is None or selected_cluster is None:
      # raise exception for cluster retrieval and stop the pipeline
      self.log.message(log_message = f"Error in receiving cluster assignement. No Cluster MetaData available for cluster: {selectedCluster}",log_message_severity = "ERROR", table_name="")
      raise exception_utility.pipeline_error

    return(cluster)
  
  def receive_tables(self, selected_cluster = None):
    # Methode for old Implementation
    tables = []
    # check if input is a list otherwise transform from string to list based on seperator
    if(not isinstance(selected_cluster,list)):
      selected_cluster = selected_cluster.replace(" ","").split(",")
      
    cluster = self._get_cluster_meta_data(selected_cluster)
    expected_cluster_schema = ["cluster_name","tables","cluster_description","compute_category","schedule","content"]
    
    # loop over cluster assignements, check the structure of each cluster, extract tables of each cluster
    for entry in cluster:
      if not set().issubset(entry.keys()):
        self.utility.message(log_message = f"Error in receiving cluster assignement. Metadata structure do not match the expected structure. MetaData: {','.join(entry.keys())}, Expected structure: {','.join(expected_schema)}",log_message_severity = "ERROR",table_name = "")
        raise PipelineError
      tables.extend(entry.get("Tables"))
    return(tables)

  #Methode for new implementation
  def receive_pipelines(self, selected_cluster = None):
    pipelines = []
    # check if input is a list otherwise transform from string to list based on seperator
    if(not isinstance(selected_cluster,list)):
      selected_cluster = selected_cluster.replace(" ","").split(",")
      
    cluster = self._get_cluster_meta_data(selected_cluster)
    expected_cluster_schema = ["cluster_name","pipeline_id","cluster_description","compute_category","schedule","content"]
    
    # loop over cluster assignements, check the structure of each cluster, extract tables of each cluster
    for entry in cluster:
      if not set().issubset(entry.keys()):
        self.utility.message(log_message = f"Error in receiving cluster assignement. Metadata structure do not match the expected structure. MetaData: {','.join(entry.keys())}, Expected structure: {','.join(expectedSchema)}",log_message_severity = "ERROR",table_name = "")
        raise exception_utility.pipeline_error
      pipelines.extend(entry.get("pipeline"))
    return(pipelines)
    