import global_utils.cosmos_db as cosmos_utility
import global_utils.log as log_utility 
import global_utils.exceptions as exception_utility
from pyspark.dbutils import DBUtils 
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

class meta_data_generation():
  def __init__(self, new_pipeline = ""):
    self.new_pipeline = new_pipeline
    self.cosmos_db = cosmos_utility.cosmos_db_interface()
    self.log = log_utility.global_loging()
    self.cosmos_meta_data = {}
    self.tables = {}
    self.read_cosmos_meta_data()
    self.module_meta_data = []
    
  def read_cosmos_meta_data(self):
    if self.new_pipeline == "":
      self.cosmos_meta_data = {"table_meta_data": self.cosmos_db.get_meta_data("databricks-pipeline-bw-metadata"),
                               "cluster_assignment": self.cosmos_db.get_meta_data("databricks-cluster-bw-assignment"),
                               "data_flow_definition": self.cosmos_db.get_meta_data("databricks-dataflow-bw-definition"),
                               "pipeline_definition": self.cosmos_db.get_meta_data("databricks-pipeline-bw-definition"),
                               "module_meta_data": self.cosmos_db.get_meta_data("databricks-pipeline-module-metadata")}
    else:
      self.cosmos_meta_data = {"table_meta_data": self.cosmos_db.get_meta_data("databricks-fact-tables"),
                               "cluster_assignment": self.cosmos_db.get_meta_data("databricks-cluster-bw-assignment"),
                               "data_flow_definition": self.cosmos_db.get_meta_data("databricks-dataflow-bw-definition"),
                               "pipeline_definition": self.cosmos_db.get_meta_data("databricks-pipeline-bw-definition"),
                               "module_meta_data": self.cosmos_db.get_meta_data("databricks-pipeline-module-metadata")}
  
  def get_collection_from_cosmos(self, cosmos_container_name: str) -> dict:
    cosmos_endpoint = dbutils.secrets.get(scope="elsa-scope",key="cosmosDB-processing-metastore-endpoint") #-> Move to config
    cosmos_masterKey = dbutils.secrets.get(scope="elsa-scope",key="cosmosDB-processing-metastore-masterKey") #-> Move to config
    client = cosmos_client.CosmosClient(cosmos_endpoint, {'masterKey': cosmos_masterKey})
    db_client = client.get_database_client("metadata")
    container_client = db_client.get_container_client(cosmos_container_name)
    
    return collection
  
  def value_exist_in_md(self, meta_data_dictionary: dict, attributes: list):
    missing_attributes = []
    for att in attributes:
      if meta_data_dictionary.get(att) is None:
        missing_attributes.append(att)
    
    if len(missing_attributes) > 0 :
      # create log with error
      [self.log.message(log_message = "No meta data available for selected tables.",log_message_severity = "ERROR", table_name = entry) for entry in missing_attributes]

  def _get_fields_from_meta_data_entry(self, meta_data_entry: dict, selected_fields: list) -> dict:
    """
    Extracts specific attributes from metadata entry and log any missing attribute.
    
    Args:
      meta_data_entry (dict): 1 object entry of the metadata
      selected_fields (list): attributes that should be extracted from this entry
      
    Returns:
      meta_data (dict): entry with the selected attributes
    
    Raises:
      metadata_error
    """
    meta_data = {}
    for entry in selected_fields:
      meta_data[entry] = meta_data_entry.get(entry)
      # Raise and log error when metadata attribute does not exist
      if meta_data.get(entry) is None:
        self.log.message(log_message = f"Metadata selection issue field {entry} not found in metadata entry {meta_data_entry}",log_message_severity = "ERROR",table_name = [])
        raise exception_utility.metadata_error
        
    return(meta_data)
  
  def get_modules(self, module_ids: list, selected_fields: list = ["id","module_path","module_inputs"]) -> list: 
    """
      get module metadata from cosmos db container
      
      Args:
        module_ids (list): list of module ids
        selected_fields (list): attributes to receive from container
      
      Returns:
        list: list of dictionaries with metadata of the module
    """
    modules = self.cosmos_meta_data.get("module_meta_data")
    modules_exist_in_md = [entry.get("id") for entry in modules if entry.get("id") in module_ids]
    module = [entry for entry in modules if entry.get("id") in module_ids]
    
    # check if all modules exist in module metadata
    if len([entry for entry in module_ids if entry not in modules_exist_in_md]) > 0:
      [self.log.message(log_message = f"could not read metadata for module {entry}.", log_message_severity = "ERROR") for entry in module_ids if entry not in modules_exist_in_md]
      raise exception_utility.metadata_error
    
    # get selected attributes
    md = [self._get_fields_from_meta_data_entry(entry, selected_fields) for entry in module]
    return md
    
  def get_table_creation(self, tableName : str, selectedFields : list) -> dict:
    # Get table metadata for selected table
    tables = self.cosmos_meta_data.get("table_meta_data")
    table = next((entry for entry in tables if entry.get("table_name") == tableName), None)
    # Check whether the table exists in the metadata
    if table is None: 
      self.log.message(LOG_MESSAGE = f"No meta data available for selected table {table}",LOG_MESSAGE_SEVERITY = "ERROR",table_name = tableName)
      raise exception_utility.metadata_error
    
    #select fields from metadata
    md = self._get_fields_from_meta_data_entry(table, selected_fields)
    
    #derive storage location
    
    #if "storage_location" in selectedFields:
    #  table = self._getFieldsFromMetaDataEntry(table, ["database","table_name","table_type","target_folder"])
    #  MD["storage_location"] = self.getLocation(database = table.get("database"), 
    #                                      tableName = table.get("table_name"),
    #                                      container = table.get("table_name"),
    #                                      storageAccount = table.get("table_name"),
    #                                      targetFolder = table.get("target_folder"),
    #                                      tableType = table.get("table_type"))
      
    return(md)
    
  def get_cluster(self, selected_cluster : list):
    if(not isinstance(selected_cluster,list)):
      selected_cluster = selected_cluster.replace(" ","").split(",")
      
    cluster = self.cosmos_meta_data.get("cluster_assignment")
    
    selected_cluster = [entry.upper() for entry in selected_cluster]
    
    cluster = [entry for entry in cluster if entry.get("id").upper() in selected_cluster]
    # check if entry exist in cluster assignment metadata
    if cluster is None or selected_cluster is None:
      # raise exception for cluster retrieval and stop the pipeline
      self.log.message(log_message = f"Error in receiving cluster assignement. No Cluster MetaData available for cluster: {selectedCluster}",log_message_severity = "ERROR", table_name="")
      raise exception_utility.pipeline_error
    
    pipelines = []
    
    
    for entry in cluster:
      pipe = self._get_fields_from_meta_data_entry(entry,selected_fields = ["pipeline"]).get("pipeline")
      pipe = [self._get_fields_from_meta_data_entry(combination,selected_fields = ["pipeline","data_flow"]) for combination in pipe]
      pipelines.extend(pipe)
      
    return pipelines
  
  def get_data_flow(self, selected_data_flow : str, selected_fields : list = ["id","data_flow","flow_definition"]) -> dict:
    """
    get data flow metadata from cosmos db container

    Args:
      selected_data_flow (str): data flow id
      selected_fields (list): attributes to receive from container

    Returns:
      dict: dictionary with metadata of the data flow
    """
    data_flow = self.cosmos_meta_data.get("data_flow_definition")
    data_flow = next((flow for flow in data_flow if flow.get("data_flow") == selected_data_flow),None)
    
    # Check whether data flow is available in the metadata
    if data_flow is None:
      self.log.message(log_message = f"DataFlow {selected_data_flow} from cluster assignment does not exist in metadata.",log_message_severity = "ERROR")
      raise exception_utility.metadata_error
    
    # select fields from metadata
    md = self._get_fields_from_meta_data_entry(data_flow, selected_fields) 
    return(md)
    
  def get_pipeline(self, selected_pipeline : str, selected_fields : list = ["id","pipeline_id","steps"]) -> dict:
    """
    get pipeline metadata from cosmos db container

    Args:
      selected_pipeline (str): pipeline id
      selected_fields (list): attributes to receive from container

    Returns:
      dict: dictionary with metadata of the pipeline
    """
    pipeline = self.cosmos_meta_data.get("pipeline_definition")
    pipeline = next((pipe for pipe in pipeline if pipe.get("pipeline_id") == selected_pipeline),None)
    
    # Check whether pipeline is available in the metadata
    if pipeline is None:
      self.log.message(log_message = f"Pipeline {selected_pipeline} from cluster assignment does not exist in metadata.",log_message_severity = "ERROR")
      raise exception_utility.metadata_error
    
    # select fields from metadata
    md = self._get_fields_from_meta_data_entry(pipeline, selected_fields) 
    return(md)
    
  def get_location(self, database: str, table_name: str, container: str, storage_account: str, target_folder: str, source_system: str, table_type: str):
    
    if database == "staging":
      location = f"abfss://{container}@{storage_account}.dfs.core.windows.net/RAW"
    elif database in ("masterdata", "auth") and table_type == "masterdata":
      location = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{table_name}/"
    else:
      location = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{folder}/{table_type}/{table_name}/data"
    
    return location
  
  def derive_inputs_from_metadata(self,inputs_to_derive_from_meta_data : dict, delivered_inputs : dict, lookup_inputs : dict,modul : str) -> dict:
    """
      starts the respective metadata retrieval function and provides an interface to the pipeline class.
      
      Args:
        inputs_to_derived_from_meta_data (dict): Inputs that are delivered by the metadat functions 
        delivered_inputs (dict): Inputs that were delivered by the data flow
        lookup_inputs (dict): Inputs that are necesssary for the metadata functions
        modul (str): module name
      
      Return:
        meta_data (dict): dictionary with all inputs for the module
        
      Raises:
        metadata_error
        
    """
    
    # check if metadat needs to be retrieved
    if inputs_to_derive_from_meta_data != {}:
      
      return_meta_data = {}
      function_name = f"get_{modul}"
      lookup_inputs["selected_fields"] = inputs_to_derive_from_meta_data.keys()

      # check if metadata function for the module is available
      if hasattr(self, function_name) and callable(getattr(self, function_name)):
          func = getattr(self, function_name)
          
          # call metadata function
          try:
            return_meta_data = func(**lookup_inputs)
          except Exception as e:
            self.log.message(log_message = f"unable to call metadata function {function_name}. Following issue occurd: {str(e)}, inputs_to_derived_from_meta_data: {inputs_to_derived_from_meta_data}, delivered_inputs: {delivered_inputs}, lookup_inputs: {lookup_inputs}", log_message_severity = "ERROR")
            raise exception_utility.meta_data_error
      
      else:
          self.log(log_message = f"metadata function {function_name} was not found.", log_message_severity = "ERROR")
          raise exception_utility.meta_data_error 
    else:
      return_meta_data = inputs_to_derive_from_meta_data
      
    return(return_meta_data)
  
  def get_metadata_pipeline(self, selected_cluster : list):
    """
    receive and validate the metadata for the frame notebook.
    
    Args:
      selected_cluster (list): cluster id the metadata is generated for
    
    Returns:
      execution_ids (dict): cluster with respective list of pipeline and data flow combination
      pipelines (dict): metadata for pipelines that are necessary to process
      data_flows (dict): metadata for pipelines that are necessary to process
      modules (dict): metadata for all modules, that are part of the pipeline
      execution_status (dict): all execution ids and their respective status
    """
    # receive cluster combinations
    cluster_mappings = self.get_cluster(selected_cluster)
    # get entry from cluster mapping
    execution_status = {}
    data_flows = {}
    pipelines = {}
    modules = {}
    execution_ids = {}
    
    
    #loop over all combinations stored in the cluster
    for entry in cluster_mappings:
      # generate execution id
      entry["execution_id"] = f"{entry.get('pipeline')}_{entry.get('data_flow')}"
      try:
        self._get_fields_from_meta_data_entry(meta_data_entry = entry, selected_fields = ["pipeline","data_flow"])
      except Exception:
        execution_status[entry.get("execution_id")] = "SKIP"
      # when status for the execution id is already SKIP than the execution should be ignored
      if(execution_status.get(entry.get("execution_id")) is None):
        execution_status[entry.get("execution_id")] = "RUNNING"
        # check if all combinations are valid combinations

        # check data flow
        try:
          # check if data flow definition was already retrieved
          if data_flows.get(entry.get("data_flow")) is None:
            data_flow = self.get_data_flow(entry.get("data_flow"),selected_fields=["flow_definition"])
            # add data flow to generate a unique dict
            data_flows[entry.get("data_flow")] = data_flow
        except Exception as e:
          self.log.message(log_message = f"{entry.get('execution_id')} was skipped due to invalid data flow {entry.get('data_flow')}, error message: {str(e)}", log_message_severity = "ERROR")
          execution_status[entry.get("execution_id")] = "SKIP"

        try:
          # check if pipeline definition was already retrieved
          if pipelines.get(entry.get("pipeline")) is None:
            pipeline = self.get_pipeline(entry.get("pipeline"))
            # add pipeline to generate a unique dict
            pipelines[entry.get("pipeline")] = pipeline
        except Exception as e:
          self.log.message(log_message = f"{entry.get('execution_id')} was skipped due to invalid pipeline {entry.get('pipeline')}, error message: {str(e)}", log_message_severity = "ERROR")
          execution_status[entry.get("execution_id")] = "SKIP"

        for step in pipeline.get("steps"):

          # check if modules in step are valid
          try:
            # check if module definition was already retrieved
            if modules.get(step.get("modul")) is None:
              module = self.get_modules(step.get("modul").split(",_!&%"))
              # add module to generate a unique dict
              modules[step.get("modul")] = module
          except Exception as e:
            self.log.message(log_message = f"{entry.get('execution_id')} was skipped due to invalid modul {step.get('modul')} defined in pipeline {entry.get('pipeline')}, error message: {str(e)}", log_message_severity = "ERROR")
            execution_status[entry.get("execution_id")] = "SKIP"
          
          # check if step exist in data_flow
          try:
            if len([entry for entry in data_flow if entry.get("pipeline_step") == step.get("step")]) != 1:
              raise exception_utility.meta_data_error
          except Exception as e:
            self.log.message(log_message = f"step {step.get('step')} in pipeline {entry.get('pipeline')} does not exist in data flow {entry.get('data_flow')}", log_message_severity = "ERROR")
          
        # when all metadata are valid add execution id to be processed
        if(execution_status.get(entry.get("execution_id")) == "RUNNING"):
          execution_ids[entry.get("execution_id")] = entry
    return execution_ids, pipelines, data_flows, modules, execution_status
    
def validate_metadata_pipeline(self, execution_ids : list, pipelines : dict, data_flows : dict, modules : dict, execution_status : dict):
  
  for run in execution_ids:
    
    data_flow = data_flows.get(run.get("data_flow"))
    pipeline = pipelines.get(run.get("pipeline"))
    
    
  
  