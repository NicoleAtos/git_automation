import json
from datetime import datetime
from pyspark.sql.types import StringType, MapType
from functools import partial
from concurrent.futures import ThreadPoolExecutor
import re
from pyspark.sql import functions as F

# import globalUtilities.cosmos_db as cosmos_utility
# import globalUtilities.log as log_utility 
# import globalUtilities.receive_metadata as meta_data_utility
# import globalUtilities.table_cluster as table_cluster_utility
# import globalUtilities.exceptions as exception_utility
# import globalUtilities.notebooks as notebooks_utility

# class GlobalUtilities():
#   def __init__(self, new_pipeline = ""):
#     self.log = log_utility.global_loging()
#     self.cosmos_db_interface = cosmos_utility.cosmos_db_interface()
#     self.notebooks = notebooks_utility.notebooks()
#     self.meta_data_generation = meta_data_utility.meta_data_generation(new_pipeline = new_pipeline)
#     self.cluster_assignment = table_cluster_utility.cluster_assignement()



  
  