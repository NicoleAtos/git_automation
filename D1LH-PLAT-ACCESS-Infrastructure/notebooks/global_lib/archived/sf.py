from typing import Any
import snowflake.connector
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from operator import itemgetter
from functools import partial
from metadata_resolver import DiscoveryToSnowflakeResolver, SnowflakeToDatabricksResolver
from global_functions import upsert_delta, remove_databricks_table
from datetime import datetime


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


class Snowflake:
    def __init__(self, env: str, auth_parameter=None, max_bytes_per_trigger: int = 1000000):
        # set current environment

        self.env = env
        self.max_bytes_per_trigger = max_bytes_per_trigger
        
        def log(log_message="", log_message_severity=""):
            print(f"{log_message_severity}: {log_message}")

        self.log = log
        
        # if no manual connection parameters are provided, read them from the secrets store
        if auth_parameter is None:
            self.auth_parameter = self._receive_connection_parameter()

        self.connection, self.base_options = self._init_connection()
        self.valid_table = {}

    def _generate_auth_key(self):
        """
        generates an auth key for the snowflake connection
        """

        # create rsa key string
        rsa_key = f"""-----BEGIN ENCRYPTED PRIVATE KEY-----
                    {self.auth_parameter.get("sf_rsa_key_base")}
                    -----END ENCRYPTED PRIVATE KEY-----"""

        # write key to file and encode during read in
        dbutils.fs.put("/local_disk0/rsa_key.pem", rsa_key, True)
        with open("/local_disk0/rsa_key.pem", "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=self.auth_parameter.get("sf_passphrase").encode(),
                backend=default_backend(),
            )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return pkb

    def _create_connection(
        self,
        sf_user: str,
        sf_account: str,
        sf_warehouse: str,
        sf_role: str,
        private_key,
    ):
        """Function to open a connection to snowflake.

        Args:
            sf_user (str): snowflake user
            sf_account (str): snowflake account url
            sf_warehouse (str): snowflake warehouse
            sf_role (str): snowflake role
            private_key (_type_): private key for authentification
        """
        try:
            connection = snowflake.connector.connect(
                user=sf_user,
                private_key=private_key,
                account=sf_account,
                warehouse=sf_warehouse,
                role=sf_role,
                client_session_keep_alive=True,
            )
            return connection
        
        except Exception as e:
            self.log(
                log_message=f"Error occured while connecting to snowflake: {str(e)}",
                log_message_severity="ERROR",
            )
            # raise exception_utility.snowflake_error

        

    def _receive_connection_parameter(self) -> dict:
        """Read snowflake connection parameters from the secrets store.

        Returns:
            dict: Dictionary with all connection parameters
        """
        parameter = {}

        parameter["sf_account"] = dbutils.secrets.get(
            scope="elsa-scope", key="snowflake-account"
        )
        parameter[
            "sf_url"
        ] = f"https://{parameter.get('sf_account')}.snowflakecomputing.com"
        parameter["sf_user"] = dbutils.secrets.get(
            scope="elsa-scope", key="snowflake-rsa-user"
        )
        parameter["sf_warehouse"] = f"{self.env.upper()}_WH_CENTRAL_ETL_DATABRICKS"
        parameter["sf_role"] = f"ELSA-SF-{self.env[0].upper()}-TC-DATALOADER"
        parameter["sf_private_key"] = dbutils.secrets.get(
            scope="elsa-scope", key="sf-private-key"
        )
        parameter["sf_rsa_key_base"] = dbutils.secrets.get(
            scope="elsa-scope", key="snowflake-rsa-key"
        )
        parameter["sf_passphrase"] = parameter.get("sf_user").split("@")[0].lower()

        return parameter

    def _init_connection(self) -> dict:
        """Initialize the connection to snowflake and returns a dict with all snowflake settings

        Returns:
            dict: base_options - contains informations necessary to connect to snowflake like account url, user, pw, role
        """

        base_options = {
            "sfUrl": self.auth_parameter.get("sf_url"),
            "sfUser": self.auth_parameter.get("sf_user"),
            "sfWarehouse": self.auth_parameter.get("sf_warehouse"),
            "sfRole": self.auth_parameter.get("sf_role"),
            "pem_private_key": self.auth_parameter.get("sf_private_key"),
            "tracing": "all",
        }

        # generate authentification key
        private_key = self._generate_auth_key()

        # create connection
        connection = self._create_connection(
            sf_user=self.auth_parameter.get("sf_user"),
            sf_warehouse=self.auth_parameter.get("sf_warehouse"),
            sf_role=self.auth_parameter.get("sf_role"),
            sf_account=self.auth_parameter.get("sf_account"),
            private_key=private_key,
        )
        return connection, base_options

    def snyc_sf_options(self, table_name: str, sf_md_options: dict):
        """checks if a sync methode for the snowflake option is available and runs the sync methode

        Args:
            table_name (str): _description_
            sf_md_options (dict): dictionary with all snowflake options for the table {option_key : option_value}
        """
        # once sync function for new option is implemented, please add the option name
        implemented_options = ["cluster_keys"]

        for option in implemented_options:
            function_name = f"_sync_sf_{option}"
            # check if sync function exists and execute the sync function for the sf option
            if hasattr(self, function_name) and callable(getattr(self, function_name)):
                func = getattr(self, function_name)
                value = sf_md_options.get(option)

                # run sync function
                func(table_name, value)
            else:
                self.log(
                    log_message=f"No sync function for snowflake option {option} is implemented in the Snowflake utility class. To enable the sync pls implement it.",
                    log_message_severity="ERROR",
                )

    def _sync_sf_cluster_keys(self, table_name, sf_md_cluster_keys: list):
        """compares snowflake cluster key and cluster key in metadata and removes are adpat the cluster key in snowflake whenever it differs from the metadata entyr

        Args:
            table_name (str): full snowflake table name (catalog.schema.table)
            sf_md_cluster_keys (list): list of cluster keys defined in snowflake
        """
        # handle missing cluster key option
        sf_md_cluster_keys = [] if sf_md_cluster_keys is None else sf_md_cluster_keys
        # compare cluster keys

        # compare cluster keys
        if not self._compare_cluster_keys(
            table_name=table_name, sf_md_cluster_keys=sf_md_cluster_keys
        ):
            # remove cluster keys if metadata cluster keys are empty
            if sf_md_cluster_keys == []:
                sql = f"ALTER TABLE {table_name} DROP CLUSTERING KEY"
                log_message = f"cluster key for table {table_name} was removed: {sql}"

            # execute alter statement to addapt the cluster keys in snowflake
            else:
                cluster_keys = [f'"{key}"' for key in sf_md_cluster_keys]
                sql = f"ALTER TABLE {table_name} CLUSTER BY ({', '.join(cluster_keys)})"
                log_message = f"cluster key for table {table_name} was addapted: {sql}"

            self._run_query(sql)
            self.log(log_message=log_message, log_message_severity="INFO")

            # recheck cluster keys to verify that the sync worked
            if not self._compare_cluster_keys(
                table_name=table_name, sf_md_cluster_keys=sf_md_cluster_keys
            ):
                self.log(
                    log_message=f"Could not sync the cluster keys in snowflake for table: {table_name}, used SQL: {sql}.",
                    log_message_severity="ERROR",
                )

    def _compare_cluster_keys(self, table_name: str, sf_md_cluster_keys: list) -> bool:
        # get cluster keys from snowflake
        cluster_keys = self.get_sf_cluster_key(table_name)

        # compare cluster keys in snowflake to metadata cluster keys
        synced_cluster_keys = set(cluster_keys) == set(sf_md_cluster_keys)

        return synced_cluster_keys

    def get_sf_cluster_key(self, table_name: str) -> list:
        """Function to query the cluster keys from information_schema.tables in snowflake.

        Args:
            table_name (str): full snowflake table name (catalog.schema.table)

        Returns:
            list: list of cluster keys for table
        """

        # seperate full table name into catalog, schema and table
        check_table = self._check_table_name(table_name=table_name).get("sf_components")
        database, schema, table = itemgetter(*check_table.keys())(check_table)
        sql = f"select CLUSTERING_KEY from {database}.INFORMATION_SCHEMA.TABLES where TABLE_CATALOG = '{database}' and TABLE_SCHEMA = '{schema}' and TABLE_NAME = '{table}' limit 1"
        cluster_keys = self._run_query(sql=sql)[0][0]

        # check if query returned a result
        if cluster_keys is None:
            self.log(
                log_message=f"cannot determine the cluster key for table: {table_name}; table_catalog: {database}, table_schema: {schema}, table: {table}.",
                log_message_severity="INFO",
            )
            cluster_keys = []
        # extract columns from string
        else:
            cluster_keys = (
                cluster_keys[len("linear(") : -len(")")].replace('"', "").split(",")
            )

        return cluster_keys

    def drop_table(self, table_name: str, extraction_checkpoint: str = None):
        """Function to drop an table within the snowflake environment

        Args:
            table_name (str): Full table name (db.schema.table)
            extraction_checkpoint (str, optional): path to the checkpoint that is used to extract data from the delta table. Defaults to None.
        """
        self.log(
            log_message=f"Delete snowflake table: {table_name}",
            log_message_severity="INFO",
        )

        # delete checkpoint for extraction if exists
        if extraction_checkpoint is not None:
            self.log(
                log_message="Delete extraction checkpoint: {extraction_checkpoint}",
                log_message_severity="INFO",
            )
            dbutils.fs.rm(extraction_checkpoint, True)

        # generate drop statement
        sql = f"DROP TABLE IF EXISTS {table_name}"
        self._run_query(sql)

    def create_table(
        self,
        table_name: str,
        columns_statement: list,
        sf_options: dict,
        table_comment: str,
        primary_key: list = None,
    ) -> bool:
        """Function to create a table within the snowflake tenant"""

        # get table from full table_name
        check_table = self._check_table_name(table_name=table_name).get("sf_components")
        database, schema, table = itemgetter(*check_table.keys())(check_table)

        # check column statement
        if columns_statement is None:
            self.log(log_message=f"column statement for table {table_name} is missing.")
        else:
            columns_statement_str = ", ".join(columns_statement)
        # create cluster key sql statement
        if sf_cluster_keys := sf_options.get("cluster_keys") is not None:
            cluster_key = ",".join(
                ['"' + key + '"' for key in sf_options.get("cluster_keys")]
            )
            cluster_statement = f"CLUSTER BY ({cluster_key})"
        else:
            cluster_statement = ""

        # generate sql create statement
        ddl_statement = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_statement_str}) change_tracking = true COMMENT = '{table_comment}' {cluster_statement};"

        # add primary key to statement if available
        if primary_key is not None:
            primary_key = ['"' + key + '"' for key in primary_key]
            primary_key = ",".join([key.upper() for key in primary_key])
            ddl_statement = ddl_statement.replace(
                f"({columns_statement_str})",
                f"({columns_statement_str}, constraint PK_{table} primary key ({primary_key}))",
            )

        self.log(
            log_message=f"create snowflake table {table_name} using sql: {ddl_statement}",
            log_message_severity="INFO",
        )

        # execute sql create statement
        self._run_query(sql=ddl_statement)

    def _run_query(self, sql: str):
        """Starts any sql statement on the snowflake tenant and tracks issues that appear

        Args:
            sql (str): sql statement to be executed on snowflake
            database (str): snowflake database
            schema (str): snowflake schema
        Raises:
            exception_utility.snowflake_error: Error in snowflake sql execution
        """

        self.log(
            log_message=f"Executes snowflake query: {sql}", log_message_severity="INFO"
        )
        cur = self.connection.cursor()
        # run sql statement on snowflake tenant
        res = []
        try:
            res = cur.execute(sql).fetchall()
        except Exception as e:
            self.log(
                log_message=f"Executes snowflake query: {sql} returned an error: {str(e)}",
                log_message_severity="ERROR",
            )
        finally:
            cur.close()
            # raise exception_utility.snowflake_error

        return res

    def close_connection(self):
        self.connection.close()

    def _check_table_name(self, table_name: str, recheck: bool = False) -> dict:
        """splis table name into database,schema and table and validates whether all components + their comabination exists in snowflake and returns whether or not the table is valid.

        Args:
            table_name (str): full table name (database.schema.table)
            recheck (bool): Forces to update the check otherwise its taken from cache

        Returns:
            bool: Flag wheteher table is a valid table and exists in snowflake
        """

        # check if table was already checked
        if self.valid_table.get(table_name) is not None and not recheck:
            check_table = self.valid_table.get(table_name)
        else:
            valid_table = False
            fail_reason = None
            sf_components = {}
            # check if spliting the table in db, schema, table works
            if num_of_entries := len(table_name.split(".")) == 3:
                database, schema, table = table_name.split(".")
                sf_components = {"database": database, "schema": schema, "table": table}
                # check if database is valid
                sql = f"select * from SNOWFLAKE.INFORMATION_SCHEMA.DATABASES where DATABASE_NAME = '{database}'"
                res = self._run_query(sql=sql)

                if len(res) != 0:
                    # check if schema is valid
                    sql = f"select * from {database}.INFORMATION_SCHEMA.SCHEMATA where SCHEMA_NAME = '{schema}'"
                    res = self._run_query(sql)

                    if len(res) != 0:
                        # check if table is valid
                        sql = f"select * from {database}.INFORMATION_SCHEMA.TABLES where  TABLE_SCHEMA = '{schema}' and TABLE_NAME = '{table}'"
                        res = self._run_query(sql)

                        if len(res) != 0:
                            valid_table = True
                        else:
                            fail_reason = "table"
                            self.log(
                                log_message=f"table {table} is invalid or user does not have access."
                            )
                    else:
                        fail_reason = "schema"
                        self.log(
                            log_message=f"schema {schema} is invalid or user does not have access."
                        )

                else:
                    fail_reason = "database"
                    self.log(
                        log_message=f"database {database} is invalid or user does not have access."
                    )

            else:
                fail_reason = "split"
                self.log(
                    log_message=f"Spliting the table {table_name} into database,schema,table returned {num_of_entries} by using the split operator '.' pls use the full table name"
                )

            # assign check to cache
            check_table = {
                "valid_table": valid_table,
                "fail_reason": fail_reason,
                "sf_components": sf_components,
            }
            self.valid_table[table_name] = check_table

        return check_table

    def check_table_creation(self, table_name: str) -> bool:
        """ """
        table_to_be_created = False

        # check if table name is available
        check_table = self._check_table_name(table_name)

        # table contains either invalid format or does not exist
        if not check_table.get("valid_table"):
            # chek if table does not exist
            if check_table.get("fail_reason") == "table":
                table_to_be_created = True

        return table_to_be_created

    def get_sf_max_optime(self, table_name: str) -> str:
        """Get latest optime for table in snowflake. If table is empty function returns "0"

        Args:
            table_name (str): snowflake full table name (database.schema.table)

        Returns:
            str: max optime found in table
        """
        max_optime = "0"
        # check whether table exists in snowflake
        check_table = self._check_table_name(table_name=table_name)
        if check_table.get("valid_table"):
            # generate sql statement to get the optime
            sql = f"select max('OPTIME') from {table_name}"
            res = self._run_query(sql=sql)

        else:
            self.log(
                log_message=f" The specified snowflake table has a wrong structure or does not exist: {table_name}."
            )
            # raise exception

        return max_optime

    def get_columns_from_sf_table(self, table_name: str) -> DataFrame:
        """Returns list of columns for the snowflake table.

        Args:
            table_name (str): snowflake table name (database.schema.table)

        Returns:
            list: list of columns
        """

        # validate snowflake table and get db, schema, table
        check_table = self._check_table_name(table_name=table_name).get("sf_components")
        database, schema, table = itemgetter(*check_table.keys())(check_table)

        # get columns for snowflake staging table
        schema_query = f"SELECT * FROM {database}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{schema}'"

        sf_dtypes = (
            spark.read.format("snowflake")
            .options(**self.base_options)
            .option("sfDatabase", database)
            .option("query", schema_query)
            .load()
            .select("COLUMN_NAME", "DATA_TYPE")
            .collect()
        )

        return sf_dtypes

    def extract_data(self, dbx_table_name: str, from_optime: str = "0") -> DataFrame:
        """Extract data from a databricks delta table selecting all data that arrived after the specified optime.

        Args:
            dbx_table_name (str): full table name (database.table)
            from_optime (str): optime to start the data extraction from

        Returns:
            DataFrame: filtered data frame that can be used for extraction
        """

        # check if table exists
        if self._dbx_table_exist(dbx_table_name):

            # read data that arrived into the table after specified optime
            try:
                df = (
                    spark.readStream.format("delta")
                    .option("maxBytesPerTrigger", self.max_bytes_per_trigger)
                    .table(dbx_table_name)
                    .filter(F.col("OPTIME") > from_optime)
                )
            except Exception as e:
                self.log(
                    log_message=f"An Error occured while extracting data from databricks table {dbx_table_name} : {str(e)}"
                )
                # raise snowflake exception

        else:
            self.log(log_message="specified table for extraction does not exist: {}")

        return df
    
    
    def _read_data_with_cdf_options(self, load_type: str) -> dict[str, Any]:
        """
        Helper method to set the Change Data Feed options based on the load type.

        Args:
            load_type (str): Load type, either 'full' or 'delta'.

        Returns:
            Dict[str, Any]: A dictionary containing the streaming options.
        """

        streaming_options = {"maxBytesPerTrigger": self.max_bytes_per_trigger}

        if load_type == "full":
            streaming_options.update({"ignoreChanges": True})
        elif load_type == "delta":
            streaming_options.update({"readChangeData": True})

        return streaming_options
      
      
    def extract_data_from_table(self, dbx_table_name: str, load_type: str) -> DataFrame:
        """
        Extract data from a Databricks Delta table as a Streaming DataFrame using Change Data Feed options.

        :param dbx_table_name (str): Name of the Databricks Delta table to read from.
        :param load_type (str): The load type, either 'full' or 'delta'.
        :return: The Streaming DataFrame object.
        """

        if not self._dbx_table_exist(dbx_table_name):
            log_message = f"specified table for extraction does not exist: {dbx_table_name}"
            self.log(log_message=log_message)
            raise Exception(log_message)

        streaming_options = self._read_data_with_cdf_options(load_type)

        read_statement = (
            spark
            .readStream
            .format("delta")
            .options(**streaming_options)
            .table(dbx_table_name)
        )

        return read_statement
    

    def extract_data_from_path(self, abfss_source_path: str, load_type: str) -> DataFrame:
        """
        Extracts data from a Delta table at a given path as a Streaming DataFrame using Change Data Feed options.

        :param abfss_source_path: The path to the Delta table in ABFSS format.
        :param load_type: The load type, either 'full' or 'delta'.
        :return: The Streaming DataFrame object.
        """

        if not self._abfss_path_exist(abfss_source_path):
            log_message = f"specified ABFSS source path for extraction does not exist: {abfss_source_path}"
            self.log(log_message=log_message)
            raise Exception(log_message)

        streaming_options = self._read_data_with_cdf_options(load_type)

        read_statement = (
            spark
            .readStream
            .format("delta")
            .options(**streaming_options)
            .load(abfss_source_path)
        )

        return read_statement
    
    
    def _abfss_path_exist(self, abfss_source_path: str) -> bool:
        """
        Check if the given ABFSS source path exists.

        Args:
            abfss_source_path (str): The ABFSS source path to check.

        Returns:
            bool: True if the path exists, False otherwise.
        """

        try:
            dbutils.fs.ls(abfss_source_path)
            return True
        except Exception:
            return False
      


    def _dbx_table_exist(self, dbx_table_name: str) -> bool:
        """checks whether a specified table exists in databricks

        Args:
            dbx_table_name (str): table name of databricks table (database.table)

        Returns:
            bool: flag that indicates whether the table exists or not
        """

        check_flag = True
        try:
            spark.table(dbx_table_name)
        except Exception as e:
            check_flag = False

        return check_flag

    def cast_data_types(self, df: DataFrame, table_name: str) -> DataFrame:
        sf_dtypes = self.get_columns_from_sf_table(table_name=table_name)
        columns = [col.COLUMN_NAME.replace('"', "") for col in sf_dtypes]

        df = df.select(*columns)
        timestamp_columns = [col[0] for col in df.dtypes if col[1] == "timestamp"]
        for col in timestamp_columns:
            sf_type = [
                i.DATA_TYPE.upper()
                for i in sf_dtypes
                if i.COLUMN_NAME.upper() == col.upper()
            ]
            sf_type = sf_type[0] if sf_type else ""
            if sf_type == "TIME":
                df = df.withColumn(col, F.date_format(col, "HH:mm:ss"))

        return df

    def _append_to_sf_staging(
        self,
        df: DataFrame,
        stage_table_name: str,
        mode: str = "DELTA",
        core_table_name: str = None,
    ):
        """Appends data frame to staging snowflake table. if mode = FULL the core table will be deleted before the appending step.

        Args:
            df (DataFrame): data frame that should be pushed to snowflake
            stage_table_name (str): snowflkae stage table name (database.schema.table)
            mode (str, optional): Can be either DELTA or FULL. Defaults to "DELTA".
            core_table_name (str, optional): core table that should be deleted in case of full mode. Defaults to None.
        """

        # check snowflake table
        check_table = self._check_table_name(table_name=stage_table_name).get("sf_components")
        database, schema, table = itemgetter(*check_table.keys())(check_table)

        # clear core table if load mode is full
        if mode == "FULL":
            if core_table_name is None:
                self.log(
                    log_message="To push data to snowflake in full mode. The core table input is required."
                )
            else:
                self.delete_table(table_name=core_table_name)

        snowflake_additional_options = {
            "sfDatabase": database,
            "sfSchema": schema,
            "dbtable": table,
        }

        options = {**self.base_options, **snowflake_additional_options}

        # overwrite staging table
        df.write.format("snowflake").options(**options).mode("overwrite").save()

    def push_to_snowflake(
        self,
        df: DataFrame,
        stage_table_name: str,
        core_table_name: str,
        primary_key: list,
        snowflake_checkpoint: str,
    ):
        """
        Pushes a DataFrame to Snowflake using a streaming query with upserts.

        :param df: The input DataFrame to be pushed to Snowflake.
        :param stage_table_name: The name of the Snowflake staging table.
        :param core_table_name: The name of the Snowflake core table.
        :param primary_key: A list of primary key column names for upserts.
        :param snowflake_checkpoint: Optional checkpoint location for the streaming query.
                                    If provided, this will enable exactly-once processing semantics.
                                    Default is None.

        :return: The streaming query object.
        """

        # Initialize the write stream with the upsert method and trigger settings
        write_stream = (
            df.writeStream.trigger(availableNow=True)
            .foreachBatch(
                partial(
                    self._upsert_snowflake,
                    stage_table_name,
                    core_table_name,
                    primary_key,
                )
            )
            .option("checkpointLocation", snowflake_checkpoint)
        )

        # Start the streaming query with the specified core table name
        streaming_query = (
            write_stream.queryName(f"{core_table_name}")
            .start()
        )

        return streaming_query
    

    def _upsert_snowflake(
            self,
            df,
            batch_id,
            stage_table_name: str,
            core_table_name: str,
            primary_key: list,
            mode: str = "DELTA"
        ):
        """function to process a micro batch within a structured stream. Pushes an df to snowflake using a staging table and merge the batch to the core table.

        Args:
            df (_type_): _description_
            stage_table_name (str): _description_
            core_table_name (str): _description_
            primary_key (list): _description_
        """
        spark.sparkContext.setLocalProperty(
            "spark.scheduler.pool", f"{core_table_name}"
        )

        # Filter the DataFrame based on the _change_type column, if present
        if "_change_type" in df.columns:
            df = df.where("_change_type != 'update_preimage'")

        # upsert data to staging table
        self._append_to_sf_staging(df, stage_table_name, mode=mode)

        # merge from staging to core table
        self._merge_stage_to_core(
            stage_table_name=stage_table_name,
            core_table_name=core_table_name,
            primary_key=primary_key,
        )

    def generate_compounded_keys(self, df: DataFrame, primary_key: list) -> DataFrame:
        """Enhance dataframe an additional field CONCAT which is a concatination of all primary keys seperated by "-". This field can be used on snowflake to optimize join performance.

        Args:
            df (DataFrame): input data frame
            primary_key (list): primary keys of table

        Returns:
            DataFrame: data frame enriched with the column
        """
        # add concat key if multiple columns are available for the primary key
        if primary_key is not None:
            if len(primary_key) > 1:
                df = df.withColumn("CONCAT", F.concat_ws("-", *primary_key))

        return df

    def delete_table(self, table_name: str):
        """Function to delete a table in snowflake

        Args:
            table_name (str): full table name (database.schema.table)
        """

        # validate the table
        self._check_table_name(table_name=table_name)

        # generate und run delete statement
        sql = f"delete from {table_name}"
        self._run_query(sql=sql)

    def generate_opt_columns(self, df: DataFrame) -> DataFrame:
        """add columns OPTYPE with constant I values and OPTIME with current timestamp.

        Args:
            df (DataFrame): input data frame

        Returns:
            DataFrame: data frame enriched with OPTIME and OPTYPE
        """

        df = df.withColumn(
            "OPTIME",
            F.date_format(
                F.to_timestamp(F.current_timestamp(), "MMddyyyy HH:mm:ss"),
                format="yyyyMMddHHmmss",
            ),
        ).withColumn("OPTYPE", F.lit("I"))

        return df

    def _merge_stage_to_core(
        self, stage_table_name: str, core_table_name: str, primary_key: list
    ):
        """Execute merge procedure in the snowflake tenant.

        Args:
            stage_table_name (str): snowflake stage table name (database.schema.table)
            core_table_name (str): snowflake core table name (database.schema.table)
        """

        merge_query = f"""
        CALL {self.env.upper()}_DB_METADATA.SCH_UTILITY.SPARK_MERGE_TABLES(
        array_construct({','.join([f"'{key.upper()}'" for key in primary_key])}),
        '{stage_table_name}',
        '{core_table_name}',
        'OPTIME'
        );"""

        # 2. - call MERGE procedure to write data into core table
        cur = self.connection.cursor().execute(merge_query)
        response = cur.fetchall()
        if any("Failed" in r[0] for r in response):
            self.log(
                log_message=f"There was an error while merging the data from snowflake {stage_table_name} to {core_table_name}"
            )
            raise Exception(response)
        
        

    def from_discovery_to_snowflake(self, metadata: dict):
        """
        Reads a Delta table as a streaming DataFrame and pushes the data to Snowflake.
        This method handles the entire process, including extracting data, applying transformations,
        and upserting the data into a Snowflake table using a stage table and a core table.

        :param metadata: A dictionary containing metadata for the Delta table and Snowflake configurations.
        :return: The streaming query object.
        """

        try:
            # Configure metadata parameters using the SFMetaHandler class
            sf_metadata = DiscoveryToSnowflakeResolver(metadata, self.env)

            # Determine the method to use for extracting data
            if sf_metadata.abfss_source_path:
                df = self.extract_data_from_path(
                    sf_metadata.abfss_source_path,
                    sf_metadata.load_type
                )
            else:
                df = self.extract_data_from_table(
                    sf_metadata.full_table_name,
                    sf_metadata.load_type
                )

            # Apply transformations, such as casting data types
            df = self.cast_data_types(df, sf_metadata.sf_core_full_table_name)

            # Push the DataFrame to Snowflake using the upsert mechanism
            streaming_query = self.push_to_snowflake(
                df,
                sf_metadata.sf_stg_full_table_name,
                sf_metadata.sf_core_full_table_name,
                sf_metadata.primary_keys,
                snowflake_checkpoint=sf_metadata.snowflake_checkpoint
            )

            # Return the streaming query object for further execution control
            return streaming_query

        except Exception as e:
            # Log the error with a detailed message
            self.log(
                log_message=(
                    "An error occurred while extracting and pushing data "
                    f"from the Databricks table {sf_metadata.full_table_name}: {str(e)}"
                )
            )           

    def _compose_snowflake_select_query(
        self,
        snowflake_full_table_name: str,
        load_type: str,
        databricks_full_table_name: str,
        increment_determining_column: str,
    ) -> str:
        """
        Composes a SELECT query for fetching data from Snowflake.

        Args:
            snowflake_full_table_name (str): Full name of the Snowflake table.
            load_type (str): Type of load operation, 'full' or 'delta'.
            databricks_full_table_name (str): Full name of the Databricks table.
            increment_determining_column (str): Column used for determining increments in delta loads.

        Returns:
            str: The composed SELECT query.
        """

        # Validate Snowflake table
        check_table = self._check_table_name(table_name=snowflake_full_table_name)
        if not check_table.get("valid_table"):
            log_message = (
                "The specified snowflake table has a wrong structure "
                f"or does not exist: {snowflake_full_table_name}.")
            self.log(log_message=log_message)
            raise Exception(log_message)
            

        query = f"SELECT * FROM {snowflake_full_table_name}"
        if load_type == "delta" and spark.catalog.tableExists(databricks_full_table_name):
            max_time = self._get_max_time_from_databricks_table(
                databricks_full_table_name, increment_determining_column
            )
            if max_time is not None:
                query += f" WHERE TO_NUMERIC({increment_determining_column}) > TO_NUMERIC('{max_time}')"
        return query

    def _get_max_time_from_databricks_table(
        self, databricks_full_table_name: str, increment_determining_column: str
    ) -> str:
        """
        Retrieves the maximum value of the increment_determining_column from the Databricks table.

        Args:
            databricks_full_table_name (str): Full name of the Databricks table.
            increment_determining_column (str): Column used for determining increments in delta loads.

        Returns:
            str: The maximum value of the increment_determining_column.
        """
        max_time = str(
            spark.read.table(databricks_full_table_name)
            .select(increment_determining_column)
            .withColumn(increment_determining_column, F.col(increment_determining_column).cast("decimal(20,6)"))
            .groupBy()
            .max(increment_determining_column)
            .collect()[0][0]
        )
        return max_time

    def _prepare_snowflake_dataframe(self, query: str, sf_database: str, snowflake_base_options: dict):
        """
        Prepares a DataFrame by executing the given query in Snowflake.

        Args:
            query (str): The SELECT query to be executed in Snowflake.
            sf_database (str): The Snowflake database to connect to.
            snowflake_base_options (dict): Base options for the Snowflake connection.

        Returns:
            DataFrame: The DataFrame containing data fetched from Snowflake.
        """
        snowflake_df = (
            spark.read.format("snowflake")
            .options(snowflake_base_options)
            .option("sfDatabase", sf_database)
            .option("query", query)
            .load()
        )
        return snowflake_df

    def _prepare_databricks_columns_statement(
        self,
        object_name: str,
        metadata_absolute_path: str
    ):
        """
        Prepares the column statement for creating a Databricks table.

        Args:
            object_name (str): The name of the Snowflake object.
            metadata_absolute_path (str): The absolute path of the metadata folder.

        Returns:
            str: The column statement for creating a Databricks table.
        """
        table_ddl = (
            spark.read.option("header", True)
            .option("sep", ";")
            .csv(f"file:///{metadata_absolute_path}/DDL/DDL_metadata_baypal.csv")
            .filter(f"table_name = '{object_name}'")
            .collect()
        )
        
        columns_statement = ",".join(
            [f"{item['column_name']} {item['source_type']}" for item in table_ddl]
            )
        
        return columns_statement
    

    def download_to_databricks(self, metadata):
        """
        Downloads data from Snowflake to Databricks.

        Args:
            metadata (dict): Metadata information for the data transfer process.

        """
        sf_metadata = SnowflakeToDatabricksResolver(metadata, self.env)

        try:

            if sf_metadata.load_type not in ("full", "delta"):
                raise ValueError(
                    f"Invalid load_type `{sf_metadata.load_type}`. "
                    "This argument must have value either `full` or `delta`"
                )

            sf_query = self._compose_snowflake_select_query(
                sf_metadata.sf_core_full_table_name,
                sf_metadata.load_type,
                sf_metadata.full_table_name,
                sf_metadata.increment_determining_column,
            )

            snowflake_df = self._prepare_snowflake_dataframe(sf_query)

            # Prepares the Databricks table for loading data
            if sf_metadata.load_type == "full" or not spark.catalog.tableExists(sf_metadata.full_table_name):

                columns_statement = self._prepare_databricks_columns_statement(
                    sf_metadata.sf_name,
                    sf_metadata.metadata_absolute_path
                )

                # Clean up
                remove_databricks_table(
                    sf_metadata.data_location,
                    sf_metadata.checkpoint,
                    sf_metadata.full_table_name
                )

                # Generate SQL query for table creation
                ddl_query = sf_metadata._resolve_ddl_statement(col_statements=columns_statement)

                # Create table
                spark.sql(ddl_query)

            # Write to databricks
            if sf_metadata.load_type == "full":
                snowflake_df.write.mode("overwrite").saveAsTable(sf_metadata.full_table_name)
            else:
                upsert_delta(
                    db=sf_metadata.databricks_database,
                    source_system=sf_metadata.source_system,
                    table_name=sf_metadata.sf_name,
                    source_type=None,
                    partition_columns=[],
                    primary_key=sf_metadata.primary_keys,
                    load_type="delta",
                    microBatchDF=snowflake_df,
                    upsert_time_column="DBX_OPTIME_2",
                    time_order_column=sf_metadata.increment_determining_column
                )

        except Exception as e:
            # Log the error with a detailed message
            self.log(
                log_message=(
                    "An error occurred while downloading data from Snowflake to Databricks."
                    f"[{sf_metadata.full_table_name}]: {str(e)}"
                )
            )


# snow = Snowflake("DEV")

# Test Sync

# snow.snyc_sf_options(
#    table_name="DEV_DB_GENERAL.SCH_CORE_H2R.T_BIC_AFIGLA_XTM2",
#    sf_md_options={"cluster_keys": ["/BIC/DBSTYPE"]},
# )

# snow._check_table_name("SCH_CORE_H2R.T_COPA_BASE_FULL_001")

# Test CONCAT column
# df = spark.table("generaldiscovery_masterdata.H2R_BIC_AMDMAAE0032")
# snow.generate_compounded_keys(
#    df=df, primary_key=["DISTR_CHAN", "MAT_SALES", "SALESORG"]
# ).display()

# print(snow._dbx_table_exist("generaldiscovery_masterdata.H2R_BIC_AMDMAAE0032"))
# print(snow._dbx_table_exist("generaldiscovery_masterdata.H2R_BIC_AMDMAAE0031"))


# snow.close_connection()

# steps for migration
# write metadata functions to generate fact tables for snowfake based on csv list
# finish functions in sf class to migrate code from notebook
# create module push to snowflake
# write snowflake temp pipeline
# make sure metadata generation functions create correct table structure (add optype / optime, divested data, CONCAT column)
# think about a way to extract databricks tables without optype and optime
