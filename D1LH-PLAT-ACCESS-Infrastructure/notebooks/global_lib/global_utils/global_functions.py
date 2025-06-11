"""
Module: Global Utils
Purpose: In this script there are several functions which have general a miscellaneuos purpose, where
Version: 1.0
Author: Dmytro Ilienko
Owner: Dmytro Ilienko
Email: dmytro.ilienko.ext@bayer.com
Dependencies: None
Usage: Every function should be called and pass the correct parameters
Reviewers: None
History:
    Date: 2024-09-03, Version: 1.0, Author: Dmytro Ilienko, Description: Creation of the script
    Date: 2025-05-28, Version: 1.1, Author: Akash Ghadage,  Description: Modified the base_path function to handle the CSW source case.
"""

from itertools import groupby
import os
import re
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession, Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from datetime import datetime
from delta import DeltaTable
from databricks.sdk.runtime import *

from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType




# copied from utils
def database_mapping(storage_account: str, container: str) -> str:
  """
  Function: database_mapping
  Description: Derives the correct hive database name from the provided storage account and discovery container names.
  Parameters:
     - storage_account (str): Name of the ADLS account.
     - container (str): Name of the discovery container.
  Returns: str - String representing the database name in hive.
  Author: Dmytro Ilienko
  Date: 2023-01-01
  """
  if "stelsahrdisc" in storage_account:
    db = "hr_discovery"
  else:
    db = f"generaldiscovery_{container}"
  return db

def check_table(db_name:str,table_name:str) -> bool:
  """
  Function: check_table
  Description: Checks if the table is present in the database or not.
  Parameters:
     - db_name (str): Name of the Database.
     - table_name (str): Name of the table.
  Returns: bool - True if the table is present in the database; otherwise, False.
  Author: Dmytro Ilienko
  Date: 2023-01-01
  """
  spark = SparkSession.builder.getOrCreate()
  try:
    spark.sql(f"describe table {db_name}.{table_name}")
    return True
  except:
    return False



def get_query_definition(table_name,source,container,special_columns,rename_mapping: dict={}) -> str:
    """
    Function: get_query_definition
    Description: Generate a string representing column definitions with comments for a given table.
    Parameters:
        table_name (str): The name of the table to retrieve column comments from.
        source (str): The data source name (usually a database or schema name).
        container (str): The container name, used as a prefix in the table path,
        special_columns: special columns with custom logic defined in mdetada  
        rename_mapping : A dictionary of column names to be renamed.
    Returns: str: A string with query definitions and comments formatted for SQL.
    Author: Akash Ghadage
    Date: 2025-03-03
    """

    spark = SparkSession.builder.getOrCreate()
    skip_metadata_columns =['# Partition Information', '# col_name']
    if container == 'discoveryhr':
      query = f"DESCRIBE TABLE hr_discovery.{source}_{table_name}"
    else: 
      query = f"DESCRIBE TABLE generaldiscovery_{container}.{source}_{table_name}"
   

    schema_df = spark.sql(query).collect()

    # Exclude Metadata columns coming from describe table query  '# Partition Information' or '# col_name'
    filtered_schema_df = [
        row for row in schema_df 
        if row['col_name'] not in skip_metadata_columns ]

    column_comments = {
        # If the column name exists in rename_mapping, use the mapped name, otherwise use the original name
        f"`{rename_mapping.get(row['col_name'], row['col_name'])}`": (row['comment'] or '').replace("'", "\\'")
        for row in filtered_schema_df
    }
    # hnadle special columns
    if special_columns:
      for scol in special_columns:
        column_comments[f"`{scol}`"] ='special column with custom logic'

    column_comments["`DIV_FLAG`"] = "Divested Flag value of 1 means divested relevant."

    column_definitions = [
        f"  {col.upper()} COMMENT '{column_comments[col]}'"
        if col in column_comments and column_comments[col] != ''
        else f"  {col.upper()}"
        for col in column_comments
    ]

    query_def = "(\n" + ",\n".join(column_definitions) + "\n)"

    return query_def

 
def basic_view_creation(
    table_name: str,layer_flag: str=None, discovery_storage_account_container: str=None, source_system: str=None, source_db: str=None, 
    target_db: str=None,  mode: str="initial", 
    masking_table: str="generaldiscovery_auth.dbx_masking_rules",
    rename_mapping: dict={}, **kwargs):
  """
  Function: basic_view_creation
  Description: Creates or updates a view in a specified Databricks database by applying masking rules and filters based on given parameters.
  Parameters:
     - table_name (str): The name of the source table for which the view is created.
     - discovery_storage_account_container (str): The container name from which the source data is derived.
     - source_system (str): Identifier for the source system from which the data originates.
     - source_db (str): The name of the database containing the source table.
     - target_db (str): The name of the target database where the view will be created.
     - mode (str): The mode of view creation, either 'initial' for creating new views or 'recreate' for replacing existing ones.
     - masking_table (str): The table containing rules for masking sensitive data.
     - rename_mapping (dict): A dictionary mapping original column names to new names.
     - **kwargs: Additional parameters for specifying optional features like table comments, special columns, or custom filters.
  Returns: None - The function executes a SQL command to create or update a view without returning a value.
  Author: Dmytro Ilienko
  Date: 2023-01-01
  """
  hive_table_name = f"{source_system}_{table_name}" if source_system else table_name

  # 0. Using global variables from Databricks environment
  spark = SparkSession.builder.getOrCreate()
  env = os.environ.get('env')
  
  # 1. Define head of the query, source and target databases, table comment
  if mode == "initial":
    query_head = "CREATE VIEW IF NOT EXISTS"
  elif mode == "recreate":
    query_head = "CREATE OR REPLACE VIEW"

  if source_db:
    pass
  elif discovery_storage_account_container:
    storage_account = f"stelsahrdisc{env}" if discovery_storage_account_container=="discoveryhr" else f"stelsadisc{env}"
    source_db = database_mapping(storage_account, discovery_storage_account_container)
  else:
    raise Exception("Not enough arguments provided to derive source database. Provide either `source_db` or `discovery_storage_account_container` argument!")
  
  if not target_db:
    target_db = "hr_r" if source_db=="hr_discovery" else f"{source_db}_r"

  table_comment = kwargs.get("table_comment", "")

  # 2. Generate masking mapping for current table
  masking_mapping = map(lambda x: x.asDict(), spark.table(masking_table).filter(f"UPPER(table) = '{table_name.upper()}' and not (rule is null or lower(subcategory)='no pii')").collect())
  masking_mapping = {k.upper(): list(v) for k, v in groupby(masking_mapping, key=lambda x: x['column'])}
  if discovery_storage_account_container=="discoveryhr":
    masking_mapping = {}
  
  # 2a. Find out if there is a cross-table condition in masking statements
  cross_table_conditions = [m["condition"] for mask_col in list(masking_mapping.values()) for m in mask_col if m["condition"] is not None and re.search(r"\.\w+\s*=\s*\w+\.", m["condition"])]
  if len(set(cross_table_conditions)) > 1:
    raise Exception(f"Multiple distinct cross-table conditions are not supported.")
  elif cross_table_conditions:
    # 2b. Find out which tables are needed from conditions for join (currently only 1 distinct table is supported)
    expressions = [item for condition in cross_table_conditions for item in re.split(' and | AND |=', condition)]
    join_tables = list(set([item.split(".")[0].strip().upper() for item in expressions if table_name.upper() not in item.upper() and "." in item]))
    
    if len(join_tables)>1:
      raise Exception(f"From 'Condition' field in masking rules the following tables were identified to be joined {', '.join(join_tables)}. Currently maximum 1 table is supported in cross-table conditions.")
    elif len(join_tables)==0:
      raise Exception("Cross-table condition was identified but the table cannot be found, please check the Condition for correctness.")
    else:
      # 2c. Generate cross-table join statement for masking
      # replace BIC_ to /BIC/ to match original table names
      join_metadata = spark.table("generaldiscovery_metadata.dbx_table_list").filter(f"lower(table_name)='{join_tables[0].replace('BIC_', '/BIC/').lower()}'").collect()
      source_exists = [item for item in join_metadata if item["source_system"].upper()==source_system.upper()]!=[]
      if not join_metadata:
        raise Exception(f"Table {join_tables[0]} is not found.")
      join_table = f"generaldiscovery_{join_metadata[0]['container']}_r.{source_system if source_exists else 'all'}_{join_tables[0]}_view"
      masking_join = f"left join {join_table} as {join_tables[0]} on {cross_table_conditions[0]}"
  else:
    masking_join = ""
  
  # 3. Generate select statement w/wo masking
  table_columns = {item[0].upper(): item[1] for item in spark.table(f"{source_db}.{hive_table_name}").dtypes}
    
  select_statements = []
  for column in table_columns:
    new_column_name = rename_mapping.get(column)
    if masking_mapping.get(column):
      column_statement = masking_generation(table_name, column, table_columns.get(column), masking_mapping.get(column), common_name=new_column_name)
    else:
      column_statement = f"{table_name}.`{column}` AS `{new_column_name}`" if new_column_name else f"{table_name}.`{column}`"
    select_statements.append(column_statement)

  # 4. Special columns (without need of masking!!!) are only included in views, as SQL statements; for example, when fields need to be recalculated dynamically (i.e. - not only when in/up-serting the records in the table). First used on salesdistribution.scl_o2c_idoc_sts to calculate number of days between an existing column and current day.
  special_columns = kwargs.get('special_columns')
  if special_columns:
    select_statements = select_statements + [column for column in special_columns]

  # 5. Define filters
  filters = kwargs.get("custom_filters", [])
  # If there are divested fields (i.e. DIV_FLAG is available), original query is appended with WHERE clause to exclude divested records for non-authorized users
  divested_flag = "DIV_FLAG" in spark.table(f"{source_db}.{hive_table_name}").columns
  if divested_flag:
    filters.append(f"CASE WHEN is_member('ELSA-DLDB-{env[0].upper()}-AR-POWERANALYST-DIV') or is_member('ELSA_DLDB_{env[0].upper()}_TC_DIVESTED') THEN TRUE ELSE DIV_FLAG!=1 END")
  # If there are no divested fields - add DIV_FLAG column as 0
  else:
    select_statements.append("0 as DIV_FLAG")

  filters = f"WHERE {' AND '.join(filters)}" if filters else ""

  # 6. Generate final query
  query_body = f"SELECT {','.join(select_statements)} FROM {source_db}.{hive_table_name} AS {table_name.lower()} {masking_join} {filters}"
  if layer_flag == "discovery":
    query_def = get_query_definition(table_name, source_system, discovery_storage_account_container,special_columns,rename_mapping)
    query = f"{query_head} {target_db}.{hive_table_name}_view {query_def} COMMENT '{table_comment}' AS {query_body}"
  else:
    query = f"{query_head} {target_db}.{hive_table_name}_view COMMENT '{table_comment}' AS {query_body}"
  
  # 7. Execute view creation query (only if it is different from the already existing one or not existing yet)
  if check_table(target_db, f"{hive_table_name}_view") :
    current_hive_query = spark.sql(f"describe table formatted {target_db}.{hive_table_name}_view").filter("col_name = 'View Text'").collect()[0].data_type
    if current_hive_query.strip()!=query_body.strip():
      print(f"Definition of {target_db}.{hive_table_name}_view view has changed. Re-creating...")
      spark.sql(query)
    elif mode == 'recreate_manually':
      print(f"mode = recreate_manually: Re-creating {target_db}.{hive_table_name}_view ...")
      spark.sql(query)
  else:
    print(f"View {target_db}.{hive_table_name}_view does not exist. Creating...")
    spark.sql(query)
 
    
    
def masking_generation(table_name: str, column_name: str, data_type: str, masking_details: list, common_name: str=None, **kwargs) -> str:
  """
  Function: masking_generation
  Description: Generates a SQL expression to apply masking or hashing to a specific column based on provided masking rules and conditions.
  Parameters:
     - table_name (str): The name of the table containing the column to be masked or hashed.
     - column_name (str): The name of the column that will undergo masking or hashing.
     - data_type (str): The data type of the column being processed for masking or hashing.
     - masking_details (list): A list of dictionaries containing details about the masking rules, conditions, and formats to apply.
     - common_name (str): An optional alias name to be used for the column in the final SQL expression.
     - **kwargs: Additional parameters for customizing the masking generation process.
  Returns: str - A SQL expression that applies the specified masking or hashing logic to the column.
  Author: Dmytro Ilienko
  Date: 2023-01-01
  """
  category_mapping = {
    "user information": "USERINFO",
    "consumer data": "CONSUMERDATA",
    "information security": "INFSEC",
    "hr information": "HRINFO",
    "sensitive finance": "SENSFIN"
  }
  
  masking_mapping = {
    "MASK_ALL": ".(?=.{0,}$)",
    "MASK_ALL_EXCEPT_4": ".(?=.{4,}$)",
    "MASK_DATE": ".+"
  }
  # using global variable
  env = os.environ.get('env')

  when_condition = "case"
  for mask_item in masking_details:
    assert mask_item["subcategory"].lower() in category_mapping
    role_suffix = category_mapping.get(mask_item["subcategory"].lower())
    composed_condition = f""" when not (is_member('ELSA-DLDB-{env[0].upper()}-AR-UNMASK_{role_suffix}') or is_member('ELSA_DLDB_{env[0].upper()}_TC_UNMASK_{role_suffix}'))"""
    if mask_item['condition']:
      composed_condition += f" and ({mask_item['condition']})"

    rule = mask_item["rule"]
    if rule == "HASH":
      main_body = f"sha1(cast({table_name}.`{column_name}` as string))"
    elif "MASK" in rule:
      assert rule in masking_mapping and mask_item['format']
      main_body = f"regexp_replace({table_name}.`{column_name}`, '{masking_mapping.get(rule)}', '{mask_item['format']}')"
    else:
      raise Exception("")
  
    if data_type == 'binary':
      main_body = f"CAST({main_body} AS BINARY)"
      
    when_condition += f"{composed_condition} then {main_body}"
  
  final_statement = f"{when_condition} else {table_name}.`{column_name}` end as `{common_name if common_name else column_name}`"
  return final_statement


def _get_most_relevant_ccn(raw_part_name: str, ccn_table, view_name: str=None, source_table: str=None, source_system: str=None):
  """
  Function: _get_most_relevant_ccn
  Description: Identifies the most relevant common column names (CCN) for a given raw column name by searching through a specified CCN table using various matching criteria.
  Parameters:
     - raw_part_name (str): The original name of the column or a part of it (prefix or suffix) to find the most relevant CCN.
     - ccn_table: The table containing entries of common column names (CCNs) and their mappings.
     - view_name (str): Optional. The name of the view to which the CCN should be applied.
     - source_table (str): Optional. The name of the source table from which the view is created.
     - source_system (str): Optional. The identifier for the source system of the data.
  Returns: list - A list of the most relevant CCNs found based on the matching criteria or an empty list if no matches are found.
  Author: Dmytro Ilienko
  Date: 2023-01-01
  """
  # Find ccn's of the column
  ccn_table_relevant_raw_col = [item for item in ccn_table if item["RAW_COLUMN_NAME"].lower()==raw_part_name.lower()]
  if len(ccn_table_relevant_raw_col) == 0:
    # raw_part_name does not have a ccn, we can skip
    return ccn_table_relevant_raw_col

  # try match views
  if view_name is not None:
    view_ccns = [item for item in ccn_table_relevant_raw_col if item["VIEW_NAME"] == view_name]
    if len(view_ccns) > 0:
      return view_ccns
  
  # try match raw_table_name and source_system combination
  if source_system is not None and source_table is not None:
    source_system_raw_table_ccns = [item for item in ccn_table_relevant_raw_col if item["RAW_TABLE_NAME"] == source_table and item["SOURCE_SYSTEM"] == source_system]
    if len(source_system_raw_table_ccns) > 0:
      return source_system_raw_table_ccns
  
  # try match raw_table_name
  if source_table is not None:
    raw_table_ccns = [item for item in ccn_table_relevant_raw_col if item["RAW_TABLE_NAME"] == source_table]
    if len(raw_table_ccns) > 0:
      return raw_table_ccns

  # try match source_system
  if source_system is not None:
    source_system_ccns = [item for item in ccn_table_relevant_raw_col if item["SOURCE_SYSTEM"] == source_system]
    if len(source_system_ccns) > 0:
      return source_system_ccns
  
  # return just general rules
  general_ccns = [item for item in ccn_table_relevant_raw_col if item["VIEW_NAME"] == "" and item["RAW_TABLE_NAME"] == "" and item["SOURCE_SYSTEM"] == ""]
  return general_ccns


def ccn_generation(raw_column_name: str, ccn_version: str, view_name: str, source_table: str, source_system: str, ccn_table):
  """
  Function: ccn_generation
  Description: Generates a common column name (CCN) for a specified raw column by resolving its name according to the best matching version from the CCN table. It can handle both full column names and those with prefixes and suffixes.
  Parameters:
     - raw_column_name (str): The original name of the column that needs to be converted into a common column name.
     - ccn_version (str): The version of the CCN to consider, either a specific number or "latest" for the most recent version.
     - view_name (str): The name of the view in which the CCN will be applied.
     - source_table (str): The source table from which the view is created.
     - source_system (str): The identifier for the source system of the data.
     - ccn_table: The table containing mappings and versions of common column names (CCNs).
  Returns: str - The fully resolved common column name, either as a complete match or a combination of resolved prefix and suffix.
  Author: Dmytro Ilienko
  Date: 2023-01-01
  """
  PREFIX_SEPARATOR = "__"
  # keep only rows available to the given version
  ccn_table_with_relevant_versions = ccn_table if ccn_version == "latest" else [item for item in ccn_table if item["VERSION"] <= int(ccn_version)]

  # try to resolve full raw column name to common column name
  column_naming = {
    item["COMMON_COLUMN_NAME"]: item["VERSION"]
    for item in _get_most_relevant_ccn(raw_column_name, ccn_table_with_relevant_versions, view_name, source_table, source_system)
  }
  
  new_column_name = max(column_naming, key=column_naming.get) if column_naming else raw_column_name

  if column_naming or not (PREFIX_SEPARATOR in raw_column_name):
    # some match of full column name was found, or impossible to split the column name -> return the resolved name
    return new_column_name
  
  # if column contains prefix separator and no rule was found for the full name, try to resolve prefix & suffix separately
  raw_prefix, raw_suffix = raw_column_name.split(PREFIX_SEPARATOR)
  
  # extract common column name of prefix
  prefix_naming = {
    item["COMMON_COLUMN_NAME"]: item["VERSION"]
    for item in _get_most_relevant_ccn(raw_prefix, ccn_table_with_relevant_versions, view_name, source_table, source_system)
  }

  # extract common prefix name of suffix
  suffix_naming = {
    item["COMMON_COLUMN_NAME"]: item["VERSION"]
    for item in _get_most_relevant_ccn(raw_suffix, ccn_table_with_relevant_versions, view_name, source_table, source_system)
  }

  common_prefix_name = max(prefix_naming, key=prefix_naming.get) if prefix_naming else raw_prefix
  common_suffix_name = max(suffix_naming, key=suffix_naming.get) if suffix_naming else raw_suffix
  final_ccn = common_prefix_name + PREFIX_SEPARATOR + common_suffix_name
  return final_ccn 


def authorization_generation(content, fields: dict=None):
  """
  Function: authorization_generation
  Description: Generates SQL authorization filters based on specified fields and content access rules, using either V_ACCESS_V1 or V_ACCESS_V2 functions.
  Parameters:
     - content (str): The content area for which authorization rules are generated.
     - fields (dict, optional): A dictionary mapping authorization field names to their respective columns in the target table. If provided, specific field-level authorizations are applied; otherwise, general content-level authorization is used.
  Returns: list - A list of SQL filter expressions that enforce the specified authorization rules.
  Author: Dmytro Ilienko
  Date: 2023-01-01
  """
  # if there are specific fields to be used for authorization - use V_ACCESS_V1 function
  if fields:
    composed_filter = []
    for auth_name, column in fields.items():
      # use the sql table function to get `/BIC/DAUTHVLOW`
      value1 = f"SELECT V_ACCESS_V1.value FROM generaldiscovery_asset_auth_r.V_ACCESS_V1('{content}', '{auth_name}')"
      value2 = f"SELECT '%' FROM generaldiscovery_asset_auth_r.V_ACCESS_V1('{content}', '{auth_name}')"
      auth_filter = f"((`{column}` IN ({value1})) OR (`{column}` LIKE ({value2} WHERE V_ACCESS_V1.value = '%')))"
      composed_filter.append(auth_filter)
  # if there are no specific fields, but the whole content area - use V_ACCESS_V2 function
  else:
    composed_filter = [f"'{content}' IN (SELECT V_ACCESS_V2.value FROM generaldiscovery_asset_auth_r.V_ACCESS_V2('{content}'))"]
  
  return composed_filter



def get_databricks_context():
    """
    Function: get_databricks_context
    Description: Initializes and returns a SparkSession and DBUtils instances, which are essential for interacting with the Databricks environment, performing data processing, and managing files.
    Parameters: None
    Returns: tuple - A tuple containing the SparkSession instance and the DBUtils instance.
    Author: Dmytro Ilienko
    Date: 2023-01-01
    """

    spark = SparkSession.builder.getOrCreate()

    dbutils = DBUtils(spark)
    
    # Return the SparkSession and DBUtils instances
    return spark, dbutils


def remove_databricks_table(data_location: str, checkpoint_location: str, table: str):
  """
  Function: remove_databricks_table
  Description: Deletes a specified Databricks table along with its associated data and checkpoint locations from Azure Blob Storage.
  Parameters:
     - data_location (str): The DBFS path where the table's data is stored.
     - checkpoint_location (str): The DBFS path for the table's checkpoint data.
     - table (str): The name of the Databricks table to be deleted.
  Returns: None - The function executes operations to remove the table and its associated data without returning any value.
  Author: Dmytro Ilienko
  Date: 2023-01-01
  """
  spark, dbutils = get_databricks_context()
  dbutils.fs.rm(data_location, True)
  dbutils.fs.rm(checkpoint_location, True)
  spark.sql(f"DROP TABLE IF EXISTS {table}")



def deduplication_function(df: DataFrame, primary_key: list, time_order_columns: list) -> DataFrame:
  """
  Function: deduplication_function
  Description: Removes duplicate rows from a DataFrame based on a set of primary key columns, retaining only the row with the latest value(s) in specified time order columns.
  Parameters:
     - df (DataFrame): The input DataFrame from which duplicates are to be removed.
     - primary_key (list): A list of columns that collectively form the primary key for identifying unique rows.
     - time_order_columns (list): A list of columns used to determine the ordering of rows; rows with the highest values in these columns are retained.
  Returns: DataFrame - A DataFrame with duplicates removed based on the primary key and time order columns.
  Author: Dmytro Ilienko
  Date: 2023-01-01
  """
  order_columns = [F.col(col).desc() for col in time_order_columns]
  windowSpec = Window.partitionBy(*primary_key).orderBy(*order_columns)
  
  return df.withColumn("rank", F.row_number().over(windowSpec)).filter(F.col("rank") == 1).drop(F.col("rank"))



def upsert_delta(db: str, source_system: str, table_name: str, source_type: str, partition_columns: list, primary_key: list, load_type: str, microBatchDF: DataFrame, upsert_time_column = 'DBX_OPTIME', time_order_column = 'OPTIME'):
  """
  Function: upsert_delta
  Description: Performs an upsert (update-insert) operation on a Delta table in Databricks, handling batch processing from a streaming DataFrame. The function deduplicates data based on primary keys and updates the table according to the specified load type.
  Parameters:
     - db (str): The name of the database containing the Delta table.
     - source_system (str): The source system identifier for the data.
     - table_name (str): The name of the Delta table.
     - source_type (str): The type of the source, which can be used to differentiate between different data sources or formats.
     - partition_columns (list): Columns used for partitioning the table.
     - primary_key (list): Primary key columns used for deduplication and merging.
     - load_type (str): Specifies the load type (e.g., 'append', 'delta', 'full') to control the upsert behavior.
     - microBatchDF (DataFrame): The incoming DataFrame representing a micro-batch of data from a stream.
     - upsert_time_column (str, optional): Column name for storing the timestamp of the upsert operation.
     - time_order_column (str, optional): Column used to determine row freshness; the most recent row is retained during upsert.
  Returns: None - The function performs an upsert operation on the Delta table without returning any value.
  Author: Dmytro Ilienko
  Date: 2023-01-01
  """
  spark, _ = get_databricks_context()
  spark.sparkContext.setLocalProperty("spark.scheduler.pool", f"{source_system}_{table_name}")
  
  microBatchDF = microBatchDF.withColumn(upsert_time_column, F.lit((datetime.utcnow()).strftime("%Y%m%d%H%M%S.%f")))
  

  if load_type=="full":
    if primary_key:
      microBatchDF = deduplication_function(microBatchDF, primary_key, time_order_columns=[time_order_column])
    microBatchDF.write.format("delta").mode("overwrite").saveAsTable(f"{db}.{source_system}_{table_name}")
  elif load_type=="append":
    microBatchDF.write.format("delta").mode("append").saveAsTable(f"{db}.{source_system}_{table_name}")
  elif load_type=="delta":
    microBatchDF = deduplication_function(microBatchDF, primary_key, time_order_columns=[time_order_column])
    # Create merge join condition according to the list of primary key columns
    pk_join_condition = ' AND '.join(f"source.`{col}` = target.`{col}`" for col in primary_key)

    # Add dynamic partition pruning
    for col in partition_columns:
      partition_values = [item[col] for item in microBatchDF.select(col).dropna().dropDuplicates().collect()]
      if partition_values:
        partition_filter = "'" + "','".join(partition_values) + "'"
        pk_join_condition += f" AND target.`{col}` in ({partition_filter})"

    baseTable = DeltaTable.forName(spark, f"{db}.{source_system}_{table_name}")
    (
      baseTable.alias("target")
      .merge(
        source = microBatchDF.alias("source"),
        condition = pk_join_condition
      )
      .whenMatchedUpdateAll(f"source.{time_order_column} > target.{time_order_column}")
      .whenNotMatchedInsertAll()
      .execute()
    )
  else:
    raise("Unsupported load type.")



def base_path(layer: str, source_system: str="", source_type: str="") -> str:
  """
  Function: base_path
  Description: Constructs the Azure Data Lake Storage (ADLS) path for a specified data layer, source system, and source type, useful for organizing and accessing data across different environments and layers.
  Parameters:
     - layer (str): The data layer (e.g., 'logging', 'staging', 'parking', 'discovery') for which the path is constructed.
     - source_system (str, optional): The source system identifier, which may be included in the path.
     - source_type (str, optional): The source type identifier, which may affect the path structure.
  Returns: str - A string representing the full ADLS path for the specified parameters.
  Author: Dmytro Ilienko
  Date: 2025-05-28
  """
  env = os.environ.get("env")
  
  if layer=="logging":
    return f"abfss://extlog-db@stelsalogs{env}.dfs.core.windows.net/stages"
  path_dict = {
    "landing": f"stelsaraw{env}",
    "staging": f"stelsaraw{env}",
    "parking": f"stelsaraw{env}",
  }


  storage_account = path_dict[layer]
  if source_system in ["GHP","SFBW","GULDA"]:
    storage_account = storage_account.replace("stelsa", "stelsahr")
  base_path = f"abfss://{layer}@{storage_account}.dfs.core.windows.net"

  if layer == "staging":
    base_path += "/RAW"
    
  if layer == "landing":
    base_path += f"/Landing{source_type}"
    if "shared" in [folder.name[:-1] for folder in dbutils.fs.ls(f"{base_path}/")] and source_system in [folder.name[:-1] for folder in dbutils.fs.ls(f"{base_path}/shared/")]:
      base_path += f"/shared/{source_system}"
    elif "CSW" in [folder.name[:-1] for folder in dbutils.fs.ls(f"{base_path}/")] and source_system in [folder.name[:-1] for folder in dbutils.fs.ls(f"{base_path}/CSW/")]:
      base_path += f"/CSW/{source_system}"
    else:
      base_path += f"/{source_system}"
  else:
    base_path += f"/{source_system}"
    
  return base_path