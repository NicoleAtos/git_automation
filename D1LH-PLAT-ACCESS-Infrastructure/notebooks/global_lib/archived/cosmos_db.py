from pyspark.dbutils import DBUtils 
from pyspark.sql import SparkSession
import globalUtilities.log as log_utility 
import globalUtilities.exceptions as exception_utility
from azure.cosmos import PartitionKey
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

class cosmos_db_interface():
  
  #initialize cosmos db connection
  def __init__(self):
    import azure.cosmos.cosmos_client as cosmos_client 
    cosmos_endpoint = dbutils.secrets.get(scope="elsa-scope",key="cosmosDB-processing-metastore-endpoint") #-> Move to config
    cosmos_masterKey = dbutils.secrets.get(scope="elsa-scope",key="cosmosDB-processing-metastore-masterKey") #-> Move to config
    client = cosmos_client.CosmosClient(cosmos_endpoint, {'masterKey': cosmos_masterKey})
    self.log = log_utility.global_loging()
    self.config = {}
    self._get_config()
    self.db_client = client.get_database_client("metadata")
    self.container_client = {}
    self.cosmos_container = {}    
    self.tables = {}
  
  def _get_list_of_containers(self) -> list:
    """
    Generate a list of containers available for the current database in cosmos db.
    
    Returns:
      list: List of cosmos db containers

    Raises:
        cosmos_error: Include any error that the function raises
    """
    try:
      containers = [table.get("id") for table in self.db_client.list_containers()]
    except Exception as e:
      self.log.message(log_message = "Could not retrieve the list of containers from cosmos db for the database metadata.", log_message_severity = "ERROR")
      raise exception_utility.cosmos_error
    return containers
  
  def _get_config(self):
    self.config["TableMD"] = "databricks-fact-tables"
  
  def _execute_query_temp(self, container : str, restriction = {}, selection = []) -> list:
    if container == "":
       return(json.load(open("/Workspace/Repos/egmgi@bayer.com/Azure-Databricks-FACT-Artifact-DevOps/notebooks/elsa/Data-pipeline-bw/GlobalFunctions/GlobalModules/ClusterAssignment.json")))
    if container == "":
      return(json.load(open("/Workspace/Repos/egmgi@bayer.com/Azure-Databricks-FACT-Artifact-DevOps/notebooks/elsa/Data-pipeline-bw/GlobalFunctions/GlobalModules/tableMD.json")))
  
  def _get_container(self, container):
    """
    Get the container client to interact with the container.
    
    Args:
      container str: container name

    Raises:
        cosmos_error: raise error if the container does not exist
    """
    if self.container_client.get(container) is None:
      # check if container exists in metadata db
      if container not in self._get_list_of_containers(): 
        self.log.message(log_message = f"Try to get client for cosmos db container that does not exist: {container}", log_message_severity = "ERROR")
        raise exception_utility.cosmos_error
      self.container_client[container] = self.db_client.get_container_client(container)
  
  # execute a query against a cosmos db container
  def _execute_query(self, container : str, restriction = {}, selection = []) -> list:
    # receive the container
    self._get_container(container)
    
    # create restriction where clause
    if restriction != {}:
      restriction = "and".join([f"r.{entry} in ('" + "','".join(restriction.get(entry)) + "')" for entry in restriction]) 
      restriction = "WHERE " + restriction
    else:
      restriction = ""
    
    # create select statement.
    if selection != []:
      selection = ",".join([f"r.{col}" for col in selection])
    else:
      selection = "*"
    
    # create query and read from cosmos db collection
    cosmos_query =  f"SELECT {selection} FROM r {restriction}"
    try:
      collection = list(self.container_client[container].query_items(
        query = cosmos_query,
        enable_cross_partition_query=True
      ))
    except Exception as e:
      log_str = f" Reading metadata from cosmosDB returned the following error: {str(e)}. Executed Query: {cosmosQuery}, Container: {container}"
      [self.log.message(log_message = log_str,log_message_severity = "ERROR", table_name = table) for table in self.tables.keys()]
      raise ValueError
    
    return(collection)
  
  def upsert_document(self, container : str, document: dict) -> bool:
    """
      upsert a dicitionary to a specific cosmos db container
      
      Args:
        container (str): cosmosDB container name
        document (dict): document that should be pushed to the container

      Raises:

    """
    # receive the container
    self._get_container(container)
    
    # check whether id attribute is available to perform the upsert command
    if document.get("id") is None:
      self.log.message(log_message = f"Cannot upsert item to cosmos db id attribute is not availabe in document {document}", log_message_severity = "ERROR")
    else:
      # Try to upsert document to container
      try:
        self.container_client[container].upsert_item(document)
      except Exception as e:
        self.log.message(log_message = f"An Error occured while upserting the document {document} to container {container}: {str(e)}",log_message_severity = "ERROR", table_name = [])
  
  def create_container(self, container : str) -> bool: 
    """
      This function creates a new container within the cosmos db.
  
      Args:
        container (str): cosmos db container name
    """
  
    # check if container already exists
    try:
      self._get_container(container)
      self.log.message(log_message = f"the container {container} has already been existed. Pls delete the container before recreating.",log_message_severity = "ERROR", table_name = [])
    except:
      # create container
      try:
        self.db_client.create_container(container, partition_key=PartitionKey(path='/id'))
      except Exception as e:
        self.log.message(log_message = f"could not create container {container}: str{e}",log_message_severity = "ERROR", table_name = [])


  # Interface for other classes to read from cosmosDB
  def get_meta_data(self, container : str, restriction = {}, selection = []):
    return self._execute_query(container)
    
  # read the lock status within cosmos db
  def get_lock_status(self, tables = {}) -> dict:
    if tables == {}:
      tables = self.tables
    
    # read lock status from cosmos db
    table_md = self._execute_query(container = self.config.get("TableMD"), selection = ["table_name","table_locked"],restriction = {"table_name": list(tables.keys()) })    
    
    # check if all tables exist in metadata
    table_md = {table["table_name"]:table["table_locked"] for table in table_md}
    [self.log.message(log_message = "Error while determining the lock status. Table was not found in the metadata collection.", log_message_severity = "ERROR", table_name = table) for table in tables if table not in table_md.keys()]
    
    # update table dictionary based on the lock status
    return {table:"skip" if table_md.get(table) else "process" for table in tables if table in list(table_md.keys())}
    
  # Structure of statusDict : {"TableName":true or false}
  # change the lock status of a table within cosmos db
  def set_lock_status(self, tables = {}, new_lock_status = True) -> dict:
    if tables == {}:
      tables = self.tables
    
    # receive container
    container = self.config.get("TableMD")
    self._get_container(container)
    
    # check if the new lock status is valid
    if new_lock_status not in [True,False]:
      [self.log.message(log_message = f"Invalid lock status: '{new_lock_status}' set.", log_message_severity = "ERROR",  table_name = table ) for table in list(tables.keys())]
    
    # read lock status from cosmos db
    table_md = self._execute_query(container = self.config.get("TableMD"),restriction = {"table_name": list(tables.keys())}) 
        
    # set new lock status
    for entry in table_md:
      entry["table_locked"] = new_lock_status
    
    # upsert entry to cosmos db
    [self.container_client[container].upsert_item(entry) for entry in table_md]