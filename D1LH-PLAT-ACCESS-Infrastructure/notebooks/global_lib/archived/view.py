class ViewCreation():
  def __init__(self, MetaData):
    self.Log = {}
    self.MetaData = MetaData
    self.sqls = {}
    self.TableQuery = []
    self.FinalStatement = ""
  
  def __str__(self):
    return self.FinalStatement.statement
  
  def getLastStatement(self):
    if self.FinalStatement != "":
      return self.FinalStatement    
    
  # Test SQL
  def testSQL(self):
    spark.sql(self.FinalStatement.statement).display()
    
  # Set Dependency between statements
  def setDependencies(self, StepNumber): 
    depend = next(query for query in self.TableQuery if query["step"] == StepNumber).get("dependency")
    if depend is not None:
      print("set Dependency")
      #print(self.sqls[depend].statement)
      #print(self.sqls[depend].TargetColumns)
      self.sqls[StepNumber].BaseColumns = self.sqls[depend].TargetColumns
      #print(self.sqls[StepNumber].BaseColumns)
      self.sqls[StepNumber].SourceStatement = self.sqls[depend].statement
  
  # Start View creation
  def ReceiveSQL(self):
    Query = self.MetaData.get("table_clause")
    self.TableQuery = Query

    for step in Query:
      stepNumber = step["step"]
      #print(step)
      # generate new object
      self.sqls[stepNumber] = ViewUtilities(step)
      
      # receive the dependency
      self.setDependencies(stepNumber)
      
      # Apply union clause
      if step["type"] == "UNION": 
        self.sqls[stepNumber].generateUnion()
      # Apply join clause
      elif step["type"] == "JOIN":
        self.sqls[stepNumber].generateJoin()
      # Generate table select statement
      elif step["type"] == "TABLE":
        self.sqls[stepNumber].generateTable()
      # generate custom coding
      elif step["type"] == "CUSTOM":
        self.sqls[stepNumber].generateCustom()

      # Generate SQL Statement
    # Receive last object in hierarchy
    lastStep = Query[-1]["step"]
    self.FinalStatement = self.sqls[lastStep]

# COMMAND ----------

class ViewUtilities():
  def __init__(self,Metadata):
    self.whiteList = []
    self.blackList = []
    self.BaseColumns = []
    self.TargetColumns = []
    self.Log = []
    self.SourceStatement = ""
    self.statement = ""
    self.Meta = Metadata
    self.additionalColumns = {}
  def generateStatement(self):
    self.statement = f" select {self.Columns} from {self.tableClause} where {filter}"
  
  # Receive columns from table
  def _retrieveColumnsForTable(self,dbName : str, TableName : str, BaseColumns, IncludeDataType = False):
    if BaseColumns != []:
      columns = BaseColumns  
    else:
      columns =  spark.table(f"{dbName}.{TableName}").columns
  
    if self.whiteList != []: columns = self._applyWhiteList(columns)
    if self.blackList != []: columns = self._applyBlackList(columns)
    
    if IncludeDataType:
      dtypes =  spark.table(f"{dbName}.{TableName}").dtypes
      columnsDtypes = {}
      for col in columns:
        columnsDtypes[col] = next(dtype[1] for dtype in dtypes if dtype[0] == col)
      return columnsDtypes
    else:
      return columns
    
  # Exclude fields from select statement
  def _applyBlackList(self, columns : list) -> list:
    if self.blackList != []:
      self._ColumnsExistInSelection(self.blackList, columns)
      columns = [col for col in columns if col not in self.blackList]
    return columns
  
  # Check if column exist in select statement
  def _ColumnsExistInSelection(self, SelectColumns, Columns):
    WLColsMissInCols = [col for col in Columns if col not in SelectColumns]
    if len(WLColsMissInCols) > 0: 
      self._AddLog("WARNING",f"Following columns in WhiteList are not part of the view {','.join(WLColsMissInCols)}")
  
  # Select fields from select statement
  def _applyWhiteList(self, columns : list) -> list:
    if self.whiteList != []:
      self._ColumnsExistInSelection(self.whiteList, columns)
      columns = [col for col in columns if col in self.whiteList]
      
    return columns 
  
  # Add message to log
  def _AddLog(self, MessageType, Message):
    self.Log.append(Message)
    
  # Apply column renaming
  def _ColumnReplace(self, ColReplaceMetaData, Columns):
      replaceDictionary = {col["column"] : col["replace"] for col in ColReplaceMetaData}
      self.TargetColumns = self.TargetColumns + [col for col in [replaceDictionary.get(col) if col in replaceDictionary  else col for col in Columns] if col not in self.TargetColumns]
      Columns = [self.additionalColumns.get(col) if col in list(self.additionalColumns.keys())  else col for col in Columns]
      Columns = [f"`{col}` as `{replaceDictionary.get(col)}`" if col in replaceDictionary  else f"`{col}`" for col in Columns]
      return Columns
  
  # Generate the filter statement
  def _generateFilterStatement(self, FilterMetaData):
    filterStmnt = ""
    if FilterMetaData != []:
      for Filter in FilterMetaData:
        # Default operator ist 
        if Filter.get("operator") is None:
          Filter["operator"] = "in"
        values = "'"+ "','".join(Filter['values']) + "'"
        if Filter["operator"] in ["in","not in"]:
          values = f"({values})"
        if Filter["operator"] == "like":
          values = f"%{values}%"
        filterStmnt.append(f"`{Filter['column']}` {Filter['operator']} {values}")
      filterStmnt = " and ".join(filterStmnt)
      filterStmnt = "where " + filterStmnt
    return filterStmnt
  
  def _applyTableAlias(self,columns,tableAlias):
    if tableAlias != "":
      columns = [ f"{tableAlias}.{col}" for col in columns]
    return columns
  
  # Generate select statement
  def _generateColumnStatement(self, database : str, table : str, columnReplace : list, tableAlias = ""):
    columns = self._retrieveColumnsForTable(database, table, BaseColumns = [])
    
    if self.additionalColumns != {}:
        # add additional columns
        print(self.additionalColumns)
        columns.extend(list(self.additionalColumns.keys()))
        # sort columns for same order within union operation
        columns.sort()
    
    if columnReplace != []:
        columns = self._ColumnReplace(columnReplace, columns)
    else:
      self.TargetColumns = self.TargetColumns + [col for col in columns if col not in self.TargetColumns]
      columns = [f"`{col}`" for col in columns]
      columns = [self.additionalColumns.get(col.replace('`','')) if col.replace('`','') in list(self.additionalColumns.keys())  else col for col in columns]
    
    
    
    columns = self._applyTableAlias(columns,tableAlias)
    
    col_statement = ",".join(columns)
    return col_statement
  
  def _getEmptyValue(self, dtype : str):
    dtype = dtype.lower()
    # identify empty character
    if dtype in ["int","double","smallint","float"]:
      emptyCharacter = "0"
    elif "decimal" in dtype:
      emptyCharacter = "0"
    elif "date" in dtype:
       emptyCharacter = "DATE'0000'"
    elif "timestamp" in dtype:
       emptyCharacter = "TIMESTAMP'0000'"
    else:
      emptyCharacter = "''"
    return emptyCharacter
  
  def _generateEmptyColumns(self, columns : dict):
    emptyColumns = {}
    for entry in columns:
      emptyCharacter = self._getEmptyValue(columns.get(entry))
      colString = f"{emptyCharacter} as `{entry}`"
      emptyColumns[entry] = colString
    return emptyColumns

  def _compareSelectStructure(self, tables : list):
    # if stucture of all Tables do not meet, generate Empty Columns
    uniqueColumns = {}
    columnsDtypes = {}
    
    # get unique list of columns and columns of each table
    for entry in tables:
      table_name = f"{entry['database']}.{entry['table']}"
      self.whiteList = entry["white_list"]
      self.blackList = entry["black_list"] 
      columnsDtypes[table_name] = self._retrieveColumnsForTable(entry.get("database"), entry.get("table"), BaseColumns = [], IncludeDataType = True)

      # do column replacement
      if entry.get("column_replace") != []:
        replaceDictionary = {col["column"] : col["replace"] for col in entry.get("column_replace")}
        columnsDtypes[table_name] = {(replaceDictionary.get(col) if col in replaceDictionary else col):(columnsDtypes.get(table_name).get(col) if col in replaceDictionary else columnsDtypes.get(col)) for col in columnsDtypes.get(table_name)}

      # add to unique dictionary
      uniqueColumns = {**uniqueColumns, **{col : columnsDtypes.get(table_name).get(col) for col in columnsDtypes.get(table_name) if col not in uniqueColumns.keys()}}
    
    # determine the missing columns per table
    missingColumns = {}
    for entry in columnsDtypes:
      missingColumns[entry] = {col : uniqueColumns.get(col) for col in uniqueColumns if col not in list(columnsDtypes.get(entry).keys())}

    # generate empty columns
    return {col : self._generateEmptyColumns(missingColumns.get(col)) for col in missingColumns}
  
  # Get SQL statement
  def _receiveSQLStatement(self, database : str, table : str, columnReplace : list, staticFilter : list, ):
        
      select = self._generateColumnStatement(database = database,
                                       table = table,
                                       columnReplace = columnReplace)
      
      filterStmnt = self._generateFilterStatement(staticFilter)
      
      Source = self._getSourceTable(database, table)
              
      sqlStatement = f"select {select} from  {Source} {filterStmnt}"
      return sqlStatement 
    
  # create union statement
  def generateUnion(self):
    UnionMetaData = self.Meta.get("query")
    union_statement = []
    
    additionalColumns = self._compareSelectStructure(UnionMetaData)
    
    for table in UnionMetaData:
      self.whiteList = table["white_list"]
      self.blackList = table["black_list"]
      self.additionalColumns = additionalColumns.get(f"{table['database']}.{table['table']}")
      
      sqlStatment = self._receiveSQLStatement( database = table["database"], table = table["table"], columnReplace = table["column_replace"], staticFilter = table["filter"])
      union_statement.append(sqlStatment)
        
    self.statement = f"select * from ({' UNION ALL '.join(union_statement)})" 
  
  # create a custom query
  def generateCustom(self): 
    self.statement = self.Meta.get(query)
  
  # create join statement  
  def generateJoin(self):
    join_statement = ""
    if self.BaseColumns == []:
      print("fail")
    select_statement = [f" base_table.`{col}` " for col in self.BaseColumns if col != 'OPTIME']
    self.TargetColumns = self.BaseColumns
    optime_list = ["base_table.`OPTIME`"]
    base_table = self._getSourceTable("","")
    for idx, join in enumerate(self.Meta.get("query")):
      self.whiteList = join["white_list"]
      self.blackList = join["black_list"]

      table_alias = join.get("alias") if join.get("alias") else 'table'+str(idx)
      keys = " AND ".join([f"{'base_table.`'+key_left+'`' if '.' not in key_left else key_left}={table_alias}.`{key_right}`" for key_left, key_right in join["keys"].items()] + [f"{table_alias}.{filter}" for filter in join["filter"]])
      join_statement += f" {join['join_type']} JOIN {join['database']}.{join['table']} AS {table_alias} ON {keys}"
      select_statement.append(self._generateColumnStatement(database = join['database'], table = join["table"], columnReplace = join["column_replace"], tableAlias = table_alias))
      optime_list.append(f"{table_alias}.`OPTIME`")

    select_statement.append(f"GREATEST({','.join(optime_list)}) as OPTIME")
    self.statement = f" SELECT {','.join(select_statement)} FROM {base_table} AS base_table {join_statement}"
  
  # Receive the Source Table
  def _getSourceTable(self, database, table):
    if self.SourceStatement == '':
      #print(self.SourceStatement)
      Source = f"{database}.{table}"
    else:
      Source = f"({self.SourceStatement})"
    return Source
  
  # create table statement
  def generateTable(self):
    table = self.Meta
    
    self.whiteList = table["white_list"]
    self.blackList = table["black_list"]
    
    self.statement = self._receiveSQLStatement(database = table["database"], table = table["table"], columnReplace = table["column_replace"], staticFilter = table["filter"])
    