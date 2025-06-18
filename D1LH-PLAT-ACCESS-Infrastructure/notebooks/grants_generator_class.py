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
    """
    GrantManager class

    Purpose:
        Handles the parsing, processing, and execution of privilege grants 
        defined in YAML configuration files. Supports GRANT, REVOKE, and full REVOKE ALL operations.
        Uses multithreading to parallelize privilege assignments and supports test (teardown) and apply modes.
    """

    def __init__(self):
        """
        Initialize the GrantManager object.
        """
        self.env = os.environ.get("env")
        self.logger = GlobalLogger()

        self.files = dbutils.widgets.get("Files")
        self.mode = dbutils.widgets.get("Mode")
        if self.mode not in ['test', 'apply']:
            raise Exception("Mode must be either 'test' or 'apply'")

        print(f"Initialized GrantManager object with env={self.env}, mode={self.mode}")
        # self.logger.log(f"Initialized GrantManager object with env={self.env}, mode={self.mode}")

    def parse_args_from_config(self, items: list[dict]) -> dict:
        """
        Parse config items from YAML input and yield normalized privilege instructions.
        """
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

            privilege_raw = item.get('privilege', '').strip().upper()

            if privilege_raw == 'REVOKE':
                action = 'REVOKE_ALL'
                privilege = None
            elif privilege_raw.startswith('REVOKE '):
                action = 'REVOKE'
                privilege = privilege_raw[7:].strip()
            else:
                action = 'GRANT'
                privilege = privilege_raw

            principals = item.get('principal', {}).get('name', [])
            if isinstance(principals, str):
                principals = [principals]

            for object_name in obj_names:
                for principal in principals:
                    if action == 'REVOKE_ALL':
                        print(f"REVOKE ALL PRIVILEGES: {object_type} {object_name} TO {principal}")
                        # self.logger.log(f"REVOKE ALL PRIVILEGES: {object_type} {object_name} TO {principal}")
                    else:
                        print(f"{item['id']} {action}: {privilege} ON {object_type} {object_name} TO {principal}")
                        # self.logger.log(f"{item['id']} {action}: {privilege} ON {object_type} {object_name} TO {principal}")
                    yield {
                        'object_type': object_type,
                        'object_name': object_name,
                        'privilege': privilege,
                        'principal': principal.format(env=self.env[0].upper()),
                        'action': action
                    }

    def load_config_yaml(self, paths: list[str]) -> list[dict]:
        """
        Load YAML configuration files.
        """
        items = []
        for path in set(paths):
            path = Path(f"../{path}")
            print(f"Loading config from: {path}")
            # self.logger.log(f"Loading config from: {path}")
            with path.open('r') as f:
                items.extend(list(yaml.safe_load_all(f)))
            f.close()
        return items

    def generate_access_statement(self, privilege, object_type, object_name, principal, revoke=False):
        """
        Build SQL GRANT or REVOKE statement.
        """
        if revoke:
            statement = f"REVOKE {privilege} ON {object_type} {object_name} FROM `{principal}`"
        else:
            statement = f"GRANT {privilege} ON {object_type} {object_name} TO `{principal}`"
        print(f"Generated statement: {statement}")
        # self.logger.log(f"Generated statement: {statement}")
        return statement

    def execute_statemet(self, statement):
        """
        Execute SQL statement.
        """
        print(f"Executing statement: {statement}")
        # self.logger.log(f"Executing statement: {statement}")
        spark.sql(statement)

    def grant_validator(self, privilege, object_type, object_name, principal, expectation):
        """
        Validate whether privilege is present or absent on object.
        Generalized for both specific privileges or total count if privilege=None.
        """
        if privilege:
            grant_count = (spark
                           .sql(f"SHOW GRANT ON {object_type} {object_name}")
                           .filter(f"ActionType='{privilege}' and Principal='{principal}'")
                           .count())
        else:
            grant_count = (spark
                           .sql(f"SHOW GRANT ON {object_type} {object_name}")
                           .filter(f"Principal='{principal}'")
                           .count())

        if grant_count != expectation:
            print(f"FAIL: expected {expectation} grants for {object_type} {object_name} to {principal}, found {grant_count}.")
            # self.logger.log(f"FAIL: expected {expectation} grants for {object_type} {object_name} to {principal}, found {grant_count}.", severity="WARNING")
        else:
            print(f"OK: {grant_count} grants for {object_type} {object_name} to {principal}.")
            # self.logger.log(f"OK: {grant_count} grants for {object_type} {object_name} to {principal}.")

    def revoke_all_privileges(self, object_type, object_name, principal):
        """
        Revoke all privileges for a principal on object.
        """
        grants = (
            spark.sql(f"SHOW GRANT ON {object_type} {object_name}")
            .filter(f"Principal = '{principal}'")
            .collect()
        )

        revoked_privileges = []
        for row in grants:
            privilege = row['ActionType']
            statement = f"REVOKE {privilege} ON {object_type} {object_name} FROM `{principal}`"
            print(f"Revoking: {statement}")
            # self.logger.log(f"Revoking: {statement}")
            spark.sql(statement)
            revoked_privileges.append(privilege)

        return revoked_privileges

    def regrant_privileges(self, privileges, object_type, object_name, principal):
        """
        Re-grant privileges (used in test teardown for REVOKE_ALL).
        """
        for privilege in privileges:
            statement = f"GRANT {privilege} ON {object_type} {object_name} TO `{principal}`"
            print(f"Re-granting: {statement}")
            # self.logger.log(f"Re-granting: {statement}")
            spark.sql(statement)

    def trigger_grant_lifecycle(self, args, teardown=False):
        """
        Main lifecycle logic: applies GRANT, REVOKE or REVOKE_ALL actions.
        """
        action = args.get('action', '')
        object_type = args['object_type']
        object_name = args['object_name']
        principal = args['principal']
        privilege = args['privilege']

        if action == 'GRANT':
            grant_statement = self.generate_access_statement(privilege, object_type, object_name, principal)
            self.grant_validator(privilege, object_type, object_name, principal, expectation=0)
            self.execute_statemet(grant_statement)
            self.grant_validator(privilege, object_type, object_name, principal, expectation=1)

            if teardown:
                revoke_statement = self.generate_access_statement(privilege, object_type, object_name, principal, revoke=True)
                self.execute_statemet(revoke_statement)
                self.grant_validator(privilege, object_type, object_name, principal, expectation=0)

        elif action == 'REVOKE':
            revoke_statement = self.generate_access_statement(privilege, object_type, object_name, principal, revoke=True)
            self.grant_validator(privilege, object_type, object_name, principal, expectation=1)
            self.execute_statemet(revoke_statement)
            self.grant_validator(privilege, object_type, object_name, principal, expectation=0)

            if teardown:
                grant_statement = self.generate_access_statement(privilege, object_type, object_name, principal)
                self.execute_statemet(grant_statement)
                self.grant_validator(privilege, object_type, object_name, principal, expectation=1)

        elif action == 'REVOKE_ALL':
            print("Starting REVOKE_ALL testing flow...")
            # self.logger.log("Starting REVOKE_ALL testing flow...")

            grants = (
                spark.sql(f"SHOW GRANT ON {object_type} {object_name}")
                .filter(f"Principal = '{principal}'")
                .collect()
            )

            if not grants:
                print(f"No privileges found to revoke for {principal} on {object_type} {object_name}.")
                # self.logger.log(f"No privileges found to revoke for {principal} on {object_type} {object_name}.")
                return

            revoked_privileges = self.revoke_all_privileges(object_type, object_name, principal)

            self.grant_validator(None, object_type, object_name, principal, expectation=0)

            if teardown:
                print("Re-granting all revoked privileges (test teardown)...")
                # self.logger.log("Re-granting all revoked privileges (test teardown)...")
                self.regrant_privileges(revoked_privileges, object_type, object_name, principal)

    def files_handling(self, files_string):
        """
        Filter valid YAML config files.
        """
        files_list = files_string.split(',')
        config_files = [file for file in files_list if file.startswith('configs/') and file.endswith('.yml')]
        print(f"Filtered config files: {config_files}")
        # self.logger.log(f"Filtered config files: {config_files}")
        return config_files

    def orchestrator(self, config_paths: list[str], teardown: bool):
        """
        Main orchestration logic — runs parsing and processing using multithreading.
        """
        print("Starting orchestration ...")
        # self.logger.log("Starting orchestration ...")

        items = self.load_config_yaml(config_paths)
        with ThreadPoolExecutor() as executor:
            results = []

            for args in self.parse_args_from_config(items):
                future = executor.submit(self.trigger_grant_lifecycle, args, teardown=teardown)
                future.add_done_callback(lambda fut, param=args: self.logger.log_async(fut, param))
                results.append(future)

        exceptions_check(results)
        print("Orchestration completed!")
        # self.logger.log("Orchestration completed!")


# COMMAND ----------

# Create instance of the class
grant_manager = GrantManager()

# Run orchestration
grant_manager.orchestrator(grant_manager.files_handling(grant_manager.files), teardown=True if grant_manager.mode == 'test' else False)
