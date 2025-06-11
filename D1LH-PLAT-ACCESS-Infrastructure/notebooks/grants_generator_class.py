# Databricks notebook source
import yaml
import sys, os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from global_lib.global_utils.logging import *
from global_lib.global_utils.helpers import *

# COMMAND ----------

dbutils.widgets.text("Files", "")
dbutils.widgets.text("Mode", "")  # test/apply

# COMMAND ----------

class GrantManager:
    
    def __init__(self):
        self.env = os.environ.get("env")
        self.logger = GlobalLogger()
        
        self.files = dbutils.widgets.get("Files")
        self.mode = dbutils.widgets.get("Mode")
        if self.mode not in ['test', 'apply']:
            raise Exception("Mode must be either 'test' or 'apply'")
        
        self.logger.(f"Initialized GrantManager object with env={self.env}, mode={self.mode}")

    def parse_args_from_config(self, items: list[dict]) -> dict:
        for item in items:
            if 'object' not in item:
                continue
            if self.env not in item['environment']:
                continue

            obj = item['object']
            object_type = obj.get('type')
            obj_names = obj.get('name', [])
            if isinstance(obj_names, str):
                obj_names = [obj_names]

            privilege = item.get('privilege')
            principals = item.get('principal', {}).get('name', [])
            if isinstance(principals, str):
                principals = [principals]

            for object_name in obj_names:
                for principal in principals:
                    self.logger.log(f"Parsed grant: {privilege} ON {object_type} {object_name} TO {principal}")
                    yield {
                        'object_type': object_type,
                        'object_name': object_name,
                        'privilege': privilege,
                        'principal': principal.format(env=self.env[0].upper())
                    }

    def load_config(self, paths: list[str]) -> list[dict]:
        items = []
        for path in set(paths):
            path = Path(f"../{path}")
            self.logger.log(f"Loading config from: {path}")
            with path.open('r') as f:
                items.extend(list(yaml.safe_load_all(f)))
            f.close()
        return items

    def generate_access_statement(self, privilege, object_type, object_name, principal, revoke=False) -> list[str]:
        if revoke:
            statement = f"REVOKE {privilege} ON {object_type} {object_name} FROM `{principal}`"
        else:
            statement = f"GRANT {privilege} ON {object_type} {object_name} TO `{principal}`"
        self.logger.log(f"Generated statement: {statement}")
        return statement

    def execute_statemet(self, statement):
        self.logger.log(f"Executing statement: {statement}")
        spark.sql(statement)

    def grant_validator(self, privilege, object_type, object_name, principal, expectation):
        grant_count = spark.sql(
            f"SHOW GRANT ON {object_type} {object_name}"
        ).filter(
            f"ActionType='{privilege}' and Principal='{principal}'"
        ).count()

        if grant_count != expectation:
            # self.logger.warning(f"FAIL: expected {expectation} grants for {object_type} {object_name} to {principal}, found {grant_count}.")
        else:
            self.logger.log(f"OK: {grant_count} grants for {object_type} {object_name} to {principal}.")

    def trigger_grant_lifecycle(self, args, teardown=False):
        grant_statement = self.generate_access_statement(**args)
        self.grant_validator(**args, expectation=0)
        self.execute_statemet(grant_statement)
        self.grant_validator(**args, expectation=1)

        if teardown:
            revoke_statement = self.generate_access_statement(**args, revoke=True)
            self.execute_statemet(revoke_statement)
            self.grant_validator(**args, expectation=0)

    def files_handling(self, files_string):
        files_list = files_string.split(',')
        config_files = [file for file in files_list if file.startswith('configs/') and file.endswith('.yml')]
        self.logger.log(f"Filtered config files: {config_files}")
        return config_files

    def orchestrator(self, config_paths: list[str], teardown: bool):
        self.logger.log("Starting orchestration...")
        items = self.load_config(config_paths)
        with ThreadPoolExecutor() as executor:
            results = []
            for args in self.parse_args_from_config(items):
                future = executor.submit(self.trigger_grant_lifecycle, args, teardown=teardown)
                future.add_done_callback(lambda fut, param=args: self.logger.log_async(fut, param))
                results.append(future)

        exceptions_check(results)
        self.logger.log("Orchestration completed.")

# COMMAND ----------

# # Create instance of the class
# grant_manager = GrantManager()

# # Run orchestration
# grant_manager.orchestrator(grant_manager.files_handling(grant_manager.files), teardown=True if grant_manager.mode == 'test' else False)
import yaml

def load_yaml_to_list_of_dicts(filepath: str) -> list[dict]:
    with open(filepath, 'r') as f:
        data = list(yaml.safe_load_all(f))
    return data

def parse_args_from_config(items: list[dict]) -> dict:
    for item in items:
        if 'object' not in item:
            continue

        obj = item['object']
        object_type = obj.get('type')
        obj_names = obj.get('name', [])
        if isinstance(obj_names, str):
            obj_names = [obj_names]

        privilege = item.get('privilege')
        principals = item.get('principal', {}).get('name', [])
        if isinstance(principals, str):
            principals = [principals]

        for object_name in obj_names:
            for principal in principals:
                print(f"Parsed grant: {privilege} ON {object_type} {object_name} TO {principal}")
                yield_item = {
                    'object_type': object_type,
                    'object_name': object_name,
                    'privilege': privilege,
                    'principal': principal.format(env=self.env[0].upper())
                }

                print(yield_item)  # <-- print before yielding

                yield yield_item

data = load_yaml_to_list_of_dicts("../configs/requests/20250606-123.yaml")
parse_args_from_config(data)