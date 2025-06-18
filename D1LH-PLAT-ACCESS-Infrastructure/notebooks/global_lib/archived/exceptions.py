class global_loging_error(Exception):
  """Raised when the type of the input is unknown"""
  def __init__(self, message = ""):
    super().__init__(message)

# Pipeline Error
class pipeline_error(Exception):
  def __init__(self):
    super().__init__("The pipeline failed due to an critical issue. Please check the logs.")
    
class metadata_error(Exception):
  def __init__(self):
    super().__init__("There was an issue while retrieving the metadata. Please check the logs.")
    
class cosmos_error(Exception):
  def __init__(self):
    super().__init__("There was an issue while communicating with the cosmos db. Please check the logs.")