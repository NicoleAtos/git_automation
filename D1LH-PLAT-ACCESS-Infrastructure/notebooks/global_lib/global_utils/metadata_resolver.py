"""
Module: Metadata Resolver
Purpose: In this script there are several functions which helps to work with ELSA metadata
Version: 1.0
Author: Mykola Nepyivoda <mykola.nepyivoda.ext@bayer.com>
Owner: Mykola Nepyivoda <mykola.nepyivoda.ext@bayer.com>
Email: Mykola Nepyivoda <mykola.nepyivoda.ext@bayer.com>
Dependencies: None
Usage: Every function should be called and pass the correct parameters
Reviewers: None
History:
    Date: 2023-04-03, Version: 1.0, Author: Mykola Nepyivoda, Description: Creation of a script
          2025-02-24, Version: 1.0, Author: Robert Kotlář, Description: Unarchivation
"""
from .global_functions import base_path


class BaseMetadataResolver:
    """
    Class: BaseMetadataResolver
    Description: Resolves metadata information for different layers in the data pipeline.
    Parameters:
        - metadata (dict): Dictionary containing metadata details.
        - env (str): Environment in which the resolver operates.
    Author: Mykola Nepyivoda
    Date: 2023-04-03
    """

    def __init__(self, metadata: dict, env: str):
        self.env = env
        self._validate_mandatory_params(metadata)
        self.source_system = metadata['source_system']
        self.table_name = metadata['table_name']
        self.load_type = metadata['load_type']
        self.databricks_col_statements = metadata.get('databricks_col_statements')
        self.table_comment = metadata.get('table_comment')
        self.primary_keys = metadata.get('primary_key')
        self.storage_account = self._resolve_storage_account(metadata)
        self.container = self._resolve_container(metadata)
        self.databricks_database = self._resolve_database_mapping()
        self.full_table_name = self._resolve_full_databricks_table_name()

    def _validate_mandatory_params(self, metadata: dict, mandatory_keys: list = [
    	'source_system', 'table_name', 'load_type']):
        """
        Function: _validate_mandatory_params
        Description: Ensures all required metadata keys are present.
        Parameters:
            - metadata (dict): Dictionary containing metadata details.
            - mandatory_keys (list): List of required keys.
        Returns: None
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        for key in mandatory_keys:
            if key not in metadata:
                raise ValueError(f"Missing mandatory key '{key}' in metadata")

    def _resolve_container(self, metadata):
        """
        Function: _resolve_container
        Description: Determines the appropriate container name based on metadata.
        Parameters:
            - metadata (dict): Dictionary containing metadata details.
        Returns: str - Container name.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        container = metadata.get("discovery_storage_account_container")
        if not container:
            container = metadata.get("container").lower() if metadata.get("source_system") not in ["GHP", "SFBW"] else "discoveryhr"
        return container

    def _resolve_storage_account(self, metadata):
        """
        Function: _resolve_storage_account
        Description: Determines the storage account name.
        Parameters:
            - metadata (dict): Dictionary containing metadata details.
        Returns: str - Storage account name.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        return metadata.get("discovery_storage_account_name", f"stelsadisc{self.env}")

    def _resolve_database_mapping(self):
        """
        Function: _resolve_database_mapping
        Description: Maps storage account details to the appropriate Hive database.
        Returns: str - Database name in Hive.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        return 'hr_discovery' if 'stelsahrdisc' in self.storage_account else f'generaldiscovery_{self.container}'

    def _resolve_full_databricks_table_name(self):
        """
        Function: _resolve_full_databricks_table_name
        Description: Constructs the full table name in Databricks.
        Returns: str - Full table name.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        return f"{self.databricks_database}.{self.source_system}_{self.table_name}"

    def _resolve_adls_data_location(self):
        """
        Function: _resolve_adls_data_location
        Description: Abstract method to be implemented in child classes to determine data location.
        Returns: None
        Raises:
            - NotImplementedError: If the function is not implemented in a child class.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        raise NotImplementedError()

    def _resolve_ddl_statement(self):
        """
        Function: _resolve_ddl_statement
        Description: Abstract method to be implemented in child classes to generate a DDL statement.
        Returns: None
        Raises:
            - NotImplementedError: If the function is not implemented in a child class.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        raise NotImplementedError()




class LandingMetadataResolver(BaseMetadataResolver):
    """
    Class: LandingMetadataResolver
    Description: Resolves metadata for the landing layer of the data pipeline.
    Parameters:
        - metadata (dict): Dictionary containing metadata details.
        - env (str): Environment in which the resolver operates.
    Author: Mykola Nepyivoda
    Date: 2023-04-03
    """

    def __init__(self, metadata: dict, env: str):
        """
        Function: __init__
        Description: Initializes the LandingMetadataResolver with metadata and environment details.
        Parameters:
            - metadata (dict): Dictionary containing metadata details.
            - env (str): Environment in which the resolver operates.
        Returns: None
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        super().__init__(metadata, env)
        self.source_type = metadata.get('source_type')
        self.data_location = self._resolve_adls_data_location()

    def _resolve_adls_data_location(self):
        """
        Function: _resolve_adls_data_location
        Description: Constructs the ADLS path for storing landing layer data.
        Returns: str - Path to the landing layer data.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        path_base = base_path('landing', self.source_system, self.source_type)
        data_location = f"{path_base}/{self.table_name}"
        return data_location


class StagingMetadataResolver(BaseMetadataResolver):
    """
    Class: StagingMetadataResolver
    Description: Resolves metadata for the staging layer of the data pipeline.
    Parameters:
        - metadata (dict): Dictionary containing metadata details.
        - env (str): Environment in which the resolver operates.
    Author: Mykola Nepyivoda
    Date: 2023-04-03
    """

    def __init__(self, metadata: dict, env: str):
        """
        Function: __init__
        Description: Initializes the StagingMetadataResolver with metadata and environment details.
        Parameters:
            - metadata (dict): Dictionary containing metadata details.
            - env (str): Environment in which the resolver operates.
        Returns: None
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        super().__init__(metadata, env)
        self.source_type = metadata.get('source_type')
        self.path_base = base_path('staging', self.source_system, self.source_type)
        self.data_location = self._resolve_adls_data_location()
        self.checkpoint_location = self._resolve_adls_checkpoint_location()

    def _resolve_adls_data_location(self):
        """
        Function: _resolve_adls_data_location
        Description: Constructs the ADLS path for storing staging layer data.
        Returns: str - Path to the staging layer data.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        return f"{self.path_base}/{self.table_name}/data"

    def _resolve_adls_checkpoint_location(self):
        """
        Function: _resolve_adls_checkpoint_location
        Description: Constructs the ADLS checkpoint location for staging data.
        Returns: str - Path to the checkpoint location.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        return f"{self.path_base}/{self.table_name}/checkpoint"

    def _resolve_ddl_statement(self, col_statements=None):
        """
        Function: _resolve_ddl_statement
        Description: Generates the DDL statement for creating the staging table in Databricks.
        Parameters:
            - col_statements (str, optional): Column definitions for the table. Defaults to None.
        Returns: str - DDL statement for table creation.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        DDL_statement = f"""
            CREATE TABLE IF NOT EXISTS {self.full_table_name} ({col_statements or self.databricks_col_statements})
            USING DELTA
            LOCATION '{self.data_location}'
            COMMENT '{self.table_comment}'
        """
        return DDL_statement

    


class DiscoveryMetadataResolver(BaseMetadataResolver):
    """
    Class: DiscoveryMetadataResolver
    Description: Resolves metadata for the discovery layer of the data pipeline.
    Parameters:
        - metadata (dict): Dictionary containing metadata details.
        - env (str): Environment in which the resolver operates.
        - target_file_size (int): Target file size for Delta table properties.
    Author: Mykola Nepyivoda
    Date: 2023-04-03
    """

    def __init__(self, metadata: dict, env: str, target_file_size: int):
        """
        Function: __init__
        Description: Initializes the DiscoveryMetadataResolver with metadata, environment, and file size details.
        Parameters:
            - metadata (dict): Dictionary containing metadata details.
            - env (str): Environment in which the resolver operates.
            - target_file_size (int): Target file size for Delta table properties.
        Returns: None
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        super().__init__(metadata, env)
        self.target_file_size = target_file_size
        self._validate_mandatory_params(
            metadata, 
            mandatory_keys=[
                'discovery_storage_account_container', 
                'discovery_storage_account_name',
                'divested_fields'
            ]
        )
        self.container = metadata['discovery_storage_account_container']
        self.storage = metadata['discovery_storage_account_name']
        self.divested_fields = metadata['divested_fields']
        self.data_location = self._resolve_adls_data_location()
        self.checkpoint = self._resolve_adls_checkpoint_location()

    def _resolve_adls_data_location(self):
        """
        Function: _resolve_adls_data_location
        Description: Constructs the ADLS path for storing discovery layer data.
        Returns: str - Path to the discovery layer data.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        data_location = (
            f"abfss://{self.container}@{self.storage}"
            f".dfs.core.windows.net/{self.source_system}_{self.table_name}/data"
        )
        return data_location

    def _resolve_adls_checkpoint_location(self):
        """
        Function: _resolve_adls_checkpoint_location
        Description: Constructs the ADLS checkpoint location for discovery data.
        Returns: str - Path to the checkpoint location.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        checkpoint_location = (
            f"abfss://{self.container}@{self.storage}.dfs.core.windows.net/"
            f"{self.source_system}_{self.table_name}/checkpoint"
        )
        return checkpoint_location

    def _resolve_ddl_statement(self, col_statements=None):
        """
        Function: _resolve_ddl_statement
        Description: Generates the DDL statement for creating the discovery table in Databricks.
        Parameters:
            - col_statements (str, optional): Column definitions for the table. Defaults to None.
        Returns: str - DDL statement for table creation.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        if self.divested_fields:
            self.databricks_col_statements += ",`DIV_FLAG` int NOT NULL COMMENT 'Technical column: 1 - divested record, 0 - non-divested record.'"

        DDL_statement = f"""
            CREATE TABLE IF NOT EXISTS {self.full_table_name} ({col_statements or self.databricks_col_statements})
            USING DELTA
            LOCATION '{self.data_location}'
            COMMENT '{self.table_comment}'
        """
        partition_statement = f"PARTITIONED BY ({','.join(self.partition_columns)})" if self.partition_columns else ""
        DDL_statement += f""" TBLPROPERTIES 
        (delta.targetFileSize = {self.target_file_size}) 
        {partition_statement}"""

        return DDL_statement

    

class DiscoveryToSnowflakeResolver(DiscoveryMetadataResolver):
    """
    Class: DiscoveryToSnowflakeResolver
    Description: Resolves metadata for loading data from Databricks discovery to Snowflake.
    A child class of DiscoveryMetadataResolver for working specifically with Snowflake metadata properties
    and the global "Snowflake" class or the "discovery_to_snowflake_function" logic.
    
    Attributes:
        snowflake_database (str): The name of the target Snowflake database.
        sf_stg_full_table_name (str): The full table name for the stage table in Snowflake.
        sf_core_full_table_name (str): The full table name for the core table in Snowflake.
        abfss_source_path (str): The ABFSS source path for data extraction, if provided in metadata.
        snowflake_checkpoint (str): The checkpoint location for Snowflake streaming writes.
    Parameters:
        - metadata (dict): Dictionary containing metadata details.
        - env (str): Environment in which the resolver operates.
    Author: Mykola Nepyivoda
    Date: 2023-04-03
    """

    def __init__(self, metadata, env):
        """
        Function: __init__
        Description: Initializes the DiscoveryToSnowflakeResolver with metadata and environment details.
        Parameters:
            - metadata (dict): Dictionary containing metadata details.
            - env (str): Environment in which the resolver operates.
        Returns: None
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        super().__init__(metadata, env)
        self.snowflake_database = self._resolve_sf_database(metadata)
        self.sf_stg_full_table_name, self.sf_core_full_table_name = self._resolve_sf_table_names()
        self.abfss_source_path = self._resolve_abfss_source_path(metadata)
        self.snowflake_checkpoint = self._resolve_sf_abfss_checkpoint(metadata)

    def _resolve_sf_database(self, metadata):
        """
        Function: _resolve_sf_database
        Description: Determines the target Snowflake database.
        Parameters:
            - metadata (dict): Dictionary containing metadata details.
        Returns: str - Name of the Snowflake database.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        return metadata.get("target_db", f"{self.env.upper()}_DB_GENERAL")

    def _resolve_sf_table_names(self):
        """
        Function: _resolve_sf_table_names
        Description: Constructs the full Snowflake table names for staging and core layers.
        Returns: tuple - (staging table name, core table name).
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        sf_stg_full_table_name = f'{self.snowflake_database}.SCH_STAGE_{self.source_system}.T_{self.table_name}'
        sf_core_full_table_name = f'{self.snowflake_database}.SCH_CORE_{self.source_system}.T_{self.table_name}'
        return sf_stg_full_table_name, sf_core_full_table_name

    def _resolve_abfss_source_path(self, metadata):
        """
        Function: _resolve_abfss_source_path
        Description: Resolves the ABFSS source path for data extraction, if available.
        Parameters:
            - metadata (dict): Dictionary containing metadata details.
        Returns: str or None - ABFSS source path or None if not provided.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        source_path = metadata.get('source_path')
        if source_path:
            return (
                f"abfss://{source_path.get('container')}"
                f"@{source_path.get('account')}{self.env}.dfs.core.windows.net/"
                f"{source_path.get('folder')}"
            )
        return None

    def _resolve_sf_abfss_checkpoint(self, metadata):
        """
        Function: _resolve_sf_abfss_checkpoint
        Description: Determines the checkpoint location for Snowflake streaming writes.
        Parameters:
            - metadata (dict): Dictionary containing metadata details.
        Returns: str - Path to the Snowflake checkpoint location.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        if metadata.get('source_db') is None and metadata.get('source_path') is None:
            return (
                f"abfss://{self.container}"
                f"@{self.storage_account}.dfs.core.windows.net/"
                f"{self.source_system}_{self.table_name}/checkpoint_snowflake"
            )

        return (
            f"abfss://utilities@stelsaraw{self.env}.dfs.core.windows.net/"
            f"snowflake_checkpoints/{self.source_system}_{self.table_name}"
        )


class SnowflakeToDatabricksResolver(BaseMetadataResolver):
    """
    Class: SnowflakeToDatabricksResolver
    Description: Resolves metadata for extracting data from Snowflake and transferring it to Databricks.
    A child class of BaseMetadataResolver
    Parameters:
        - metadata (dict): Dictionary containing metadata details.
        - env (str): Environment in which the resolver operates.
    Author: Mykola Nepyivoda
    Date: 2023-04-03
    """

    def __init__(self, metadata, env):
        """
        Function: __init__
        Description: Initializes the SnowflakeToDatabricksResolver with metadata and environment details.
        Parameters:
            - metadata (dict): Dictionary containing metadata details.
            - env (str): Environment in which the resolver operates.
        Returns: None
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        super().__init__(metadata, env)
        self._validate_mandatory_params(
            metadata, mandatory_keys=[
                'container',
                'snowflake_schema',
                'snowflake_database',
                'output_storage_account',
                'metadata_absolute_path',
                'snowflake_base_options',
                'snowflake_object_prefix',
                'increment_determining_column'
            ]
        )

        self.sf_name = metadata['table_name']
        self.sf_container = metadata['container']
        self.snowflake_schema = metadata['snowflake_schema']
        self.snowflake_database = metadata['snowflake_database']
        self.output_storage_account = metadata['output_storage_account']
        self.metadata_absolute_path = metadata['metadata_absolute_path']
        self.snowflake_base_options = metadata['snowflake_base_options']
        self.snowflake_object_prefix = metadata['snowflake_object_prefix']
        self.increment_determining_column = metadata['increment_determining_column']
        self.sf_core_full_table_name = self._resolve_snowflake_full_name(metadata)

    def _resolve_snowflake_full_name(self, metadata):
        """
        Function: _resolve_snowflake_full_name
        Description: Constructs the fully qualified Snowflake table name using metadata.
        Parameters:
            - metadata (dict): Dictionary containing metadata details.
        Returns: str - Fully qualified Snowflake table name.
        Author: Mykola Nepyivoda
        Date: 2023-04-03
        """
        return (f"{metadata['snowflake_database']}.{metadata['snowflake_schema']}."
                f"{metadata['snowflake_object_prefix']}{metadata['table_name']}")
