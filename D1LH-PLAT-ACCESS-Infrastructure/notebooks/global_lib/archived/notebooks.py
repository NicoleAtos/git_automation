import global_utils.receive_metadata as meta_data_utility
import global_utils.log as log_utility 
import global_utils.exceptions as exception_utility
from pyspark.dbutils import DBUtils 
from pyspark.sql import SparkSession
import json
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

class notebooks:
  
  def __init__(self):
    self.module_input = {"pipeline_id":"", "runtime_id":""}
    self.module_output = {}
    self.module_md_inputs = []
    self.module_id = ""
    self.meta_data = meta_data_utility.meta_data_generation()
    self.log = log_utility.global_loging()
  
  def initialize_notebook(self, module_id: str ):
    """
      wrapper function to initialize the notebook and create widgets and receive input values

      Args:
        module_id (str): id of the module stored in the module repository
    """
    # set module id
    self.module_id = module_id
    # get metadata from repository
    self._get_inputs_metadata()
    self.log.message(log_message = f"input values for module {self.module_id} are: {self.module_input}", log_message_severity = "INFO")
    # create widgets
    self._create_widgets()

  def _get_inputs_metadata(self):
    """
      use metadata function to read inputs from cosmos metadata storage for a specific module id
    """
    # receive metadata    
    module_inputs = self.meta_data.get_modules(self.module_id.split(",!&"), ["module_inputs"])
    if len(module_inputs) != 1:
      self.log.message(log_message = f"received multiple or no metadata entries for module id :{self.module_id} ; {module_inputs}", log_message_severity = "ERROR")
      raise exception_utility.notebooks_error    
    
    self.module_md_inputs = module_inputs[0].get("module_inputs")


  def _create_widgets(self):
    """
      function to create all widgets based on the metadata
    """
    # Remove all widgets prevent dependencies
    dbutils.widgets.removeAll()
  
    # Loop over all widget metadata and create the widgets + read values into inputs dictionary
    for entry in self.module_md_inputs:
      input_entry = dict((k, entry[k]) for k in ["name","input_type","default_value","label"] if k in entry)
      self.module_input[entry.get("name")] = self._receive_inputs(**input_entry)
  
    self.log.message(log_message = f"initial widget values are set to {self.module_input}", log_message_severity = "INFO")

  
  def _receive_inputs(self,name: str, input_type: str, default_value: str, label: str):
    """
    Read values from widgets and parse json inputs if necessary.

    Args:
      name (str): widget name
      input_type (str): type of widget (allowed values are text, dict or list)
      default_value (str): default value of the widget
      label (str): lable of the widget

    raises:
      notebooks_error
    """

    # define widgets
    input_type = input_type.upper()
    if input_type in ["TEXT", "LIST", "DICT"]: 
      dbutils.widgets.text(name, default_value, label)    
    else:
      raise exception_utility.notebooks_error
      self.log.message(log_message = f"Input {name}: In valid type {input_type} specified in metadata. Should be TEXT, LIST or DICT", log_message_severity = "ERROR")

    # Receive Value
    value = dbutils.widgets.get(name)
    
    # parse json inputs to dict or list
    if input_type in ["LIST","DICT"]:
      try:
        value = json.loads(value)
      except ValueError as e:
        raise exception_utility.notebooks_error
        self.log.message(log_message = f"Input {name}: Expected JSON for Input Type {input_type} is not a valid json: {value}", log_message_severity = "ERROR")

      if input_type == "LIST" and type(value) != list:
        raise exception_utility.notebooks_error
        self.log.message(log_message = f"Input {name}: Translation from JSON to List was not possible: {value}", log_message_severity = "ERROR")
        
      if input_type == "DICT" and type(value) != dict:
        raise exception_utility.notebooks_error
        self.log.message(log_message = f"Input {name}: Translation from JSON to Dict was not possible: {value}", log_message_severity = "ERROR")

    return(value)

  # Send output to notebook exit 
  def return_outputs(self):
    """
      parse output dictionary to json string, stop the notebook and return the output string.
    """
    try:
      output = json.dumps(self.module_output)
    except Exception as e:
      self.log.message(log_message = f"outputs can not ", log_message_severity = "ERROR")
      raise exception_utility.notebooks_error
    dbutils.notebook.exit(output)