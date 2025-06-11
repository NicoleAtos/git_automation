# Databricks notebook source
import yaml
import sys, os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from global_lib.global_utils.logging import *
from global_lib.global_utils.helpers import *

# COMMAND ----------

dbutils.widgets.text("Files", "")
dbutils.widgets.text("Mode", "") # test/apply

# COMMAND ----------

env = os.environ.get("env")
logger = GlobalLogger()

files = dbutils.widgets.get("Files")
mode = dbutils.widgets.get("Mode")
if mode not in ['test', 'apply']:
  raise Exception("Mode must be either 'test' or 'apply'")

# COMMAND ----------

def parse_args_from_config(items: list[dict]) -> dict:
  
  for item in items:
    # skip header or malformed items
    if 'object' not in item:
      continue
    # skip if not in current env
    if env not in item['environment']:
      continue
    
    # add warnings if mandatory fields have wrong input (like typo in privilege, or non-existing object)
    obj = item['object']
    object_type = obj.get('type')
    obj_names = obj.get('name', [])
    # normalize to list
    if isinstance(obj_names, str):
        obj_names = [obj_names]

    privilege = item.get('privilege')

    principals = item.get('principal', {}).get('name', [])
    if isinstance(principals, str):
        principals = [principals]

    for object_name in obj_names:
      for principal in principals:
          yield {
              'object_type': object_type,
              'object_name': object_name,
              'privilege': privilege,
              'principal': principal.format(env=env[0].upper())
          }

# COMMAND ----------

def load_config(paths: list[str]) -> list[dict]:
  
  items = []
  for path in set(paths): # deduplicate paths
    path = Path(f"../{path}")
    with path.open('r') as f:
      items.extend(list(yaml.safe_load_all(f)))
    f.close()
  return items

# COMMAND ----------

def generate_access_statement(privilege, object_type, object_name, principal, revoke=False) -> list[str]:
    """
    Generates SQL GRANT statements.
    """
    if revoke:
      statement = f"REVOKE {privilege} ON {object_type} {object_name} FROM `{principal}`"
    else:
      statement = f"GRANT {privilege} ON {object_type} {object_name} TO `{principal}`"

    return statement

# COMMAND ----------

def execute_statemet(statement):
  # parallelize
  spark.sql(statement)

# COMMAND ----------

def grant_validator(privilege, object_type, object_name, principal, expectation):
    grant_count = spark.sql(f"SHOW GRANT ON {object_type} {object_name}").filter(f"ActionType='{privilege}' and Principal='{principal}'").count()
    if grant_count!=expectation:
      print(f"FAIL: expected {expectation} grants for {object_type} {object_name} to {principal}, found {grant_count}.")
    else:
      print(f"OK: {grant_count} grants for {object_type} {object_name} to {principal}.")

# COMMAND ----------

def trigger_grant_lifecycle(args, teardown=False):
    grant_statement = generate_access_statement(**args)

    grant_validator(**args, expectation=0)
    execute_statemet(grant_statement)
    grant_validator(**args, expectation=1)

    if teardown:
      revoke_statement = generate_access_statement(**args, revoke=True)
      execute_statemet(revoke_statement)
      grant_validator(**args, expectation=0)

# COMMAND ----------

def files_handling(files_string):
  files_list = files_string.split(',')
  config_files = [file for file in files_list if file.startswith('configs/') and file.endswith('.yml')]
  return config_files

# COMMAND ----------

# TODO
  # to convert into class
  # add support of warnings into logger (then convert print in grant_validator to warnings)

# COMMAND ----------

def orchestrator(config_paths: list[str], teardown: bool):
  
  items = load_config(config_paths)
  with ThreadPoolExecutor() as executor:
    results = []
    for args in parse_args_from_config(items):
      future = executor.submit(trigger_grant_lifecycle, args, teardown=teardown)
      future.add_done_callback(lambda fut, param=args: logger.log_async(fut, param))
      results.append(future)

  # Check for exceptions
  exceptions_check(results)

# COMMAND ----------

orchestrator(files_handling(files), teardown= True if mode=='test' else False)
