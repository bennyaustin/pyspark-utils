# Databricks notebook source
from pyspark.sql.functions import trim,when,isnull,lit,col,from_utc_timestamp,to_utc_timestamp,concat_ws,sha1,length,substring,lit,concat,date_add,expr,year,datediff
from pyspark.sql import functions as F 
import datetime

# COMMAND ----------

class CommonTransforms:
  inputDf=None
  inputSchema=None
  inputColums=None
  
#   Constructor
  def __init__(self, input):
    self.inputDf=input
    self.inputSchema=self.inputDf.schema
    self.inputColumns=self.inputDf.schema.fieldNames()
    
#  Remove Leading and Trailing Spaces 
  def trim(self):
    stringCol= (col for col in self.inputSchema if str(col.dataType)=="StringType")
    for col in stringCol:
        self.inputDf = self.inputDf.withColumn(col.name,trim(col.name))
    return self.inputDf
  
#   Replace Null values with Default values based on datatypes
  def replaceNull(self,value, subset=None):
    isDate=False
    isTimestamp =False
    
    try:
      if isinstance(value, str):
        date_obj = datetime.datetime.strptime(value, "%Y-%m-%d") #YYYY-MM-DD format e.g "2020-10-01"
        isDate= True
    except ValueError:
      isDate=False
      
    try:
      if isinstance(value, str):
        date_obj = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S") #YYYY-MM-DDThh:mm:ss format e.g "2020-10-01T19:50:06"
        isTimestamp= True
    except ValueError:
      isTimestamp=False
      
    if isDate and subset is not None:
      dateCol = (x for x in self.inputSchema if str(x.dataType)=="DateType" and x.nullable==True and x.name in subset)
      for x in dateCol:
        self.inputDf = self.inputDf.withColumn(x.name, when(isnull(col(x.name)),lit(value)).otherwise(col(x.name)))
    elif isDate and subset is None:
      dateCol = (x for x in self.inputSchema if str(x.dataType)=="DateType" and x.nullable==True)
      for x in dateCol:
        self.inputDf = self.inputDf.withColumn(x.name, when(isnull(col(x.name)),lit(value)).otherwise(col(x.name)))
    elif isTimestamp and subset is not None:
      tsCol = (x for x in self.inputSchema if str(x.dataType)=="TimestampType" and x.nullable==True and x.name in subset)
      for x in tsCol:
        self.inputDf = self.inputDf.withColumn(x.name, when(isnull(col(x.name)),lit(value)).otherwise(col(x.name)))
    elif isTimestamp and subset is None:
      tsCol = (x for x in self.inputSchema if str(x.dataType)=="TimestampType" and x.nullable==True)
      for x in tsCol:
        self.inputDf = self.inputDf.withColumn(x.name, when(isnull(col(x.name)),lit(value)).otherwise(col(x.name)))        
    else:
      self.inputDf = self.inputDf.fillna(value,subset)
      
    return self.inputDf

#  Remove duplicates
  def deDuplicate(self, subset=None):
    self.inputDf = self.inputDf.dropDuplicates(subset)
    return self.inputDf
  
#   Convert UTC timestamp to local
  def utc_to_local(self,localTimeZone,subset=None):
    if subset is not None:
      tsCol = (x for x in  self.inputSchema if str(x.dataType)=="TimestampType" and x.name in subset)
    else:
      tsCol = (x for x in  self.inputSchema if str(x.dataType)=="TimestampType")
      
    for x in tsCol:
      self.inputDf = self.inputDf.withColumn(x.name,from_utc_timestamp(col(x.name),localTimeZone))
    return self.inputDf

#   Convert timestamp in local timezone to UTC
  def local_to_utc(self,localTimeZone,subset=None):
    if subset is not None:
      tsCol = (x for x in  self.inputSchema if str(x.dataType)=="TimestampType" and x.name in subset)
    else:
      tsCol = (x for x in  self.inputSchema if str(x.dataType)=="TimestampType")
      
    for x in tsCol:
      self.inputDf = self.inputDf.withColumn(x.name,to_utc_timestamp(col(x.name),localTimeZone))
    return self.inputDf
  
#   Change Timezone
  def changeTimezone(self,fromTimezone,toTimezone,subset=None):
    if subset is not None:
      tsCol = (x for x in  self.inputSchema if str(x.dataType)=="TimestampType" and x.name in subset)
    else:
      tsCol = (x for x in  self.inputSchema if str(x.dataType)=="TimestampType")
    
    for x in tsCol:
      self.inputDf = self.inputDf.withColumn(x.name,to_utc_timestamp(col(x.name),fromTimezone))
      self.inputDf = self.inputDf.withColumn(x.name,from_utc_timestamp(col(x.name),toTimezone))
    return self.inputDf

#   Drop System/Non-Business Columns
  def dropSysColumns(self,columns):
    self.inputDf = self.inputDf.drop(columns)
    return self.inputDf 
  
#  Create Checksum Column 
  def addChecksumCol(self,colName):
    self.inputDf = self.inputDf.withColumn(colName,sha1(concat_ws("~~", *self.inputDf.columns)))
    return self.inputDf

# Convert Julian Date to Calendar Date  
  def julian_to_calendar(self,subset):
    julCol = (x for x in self.inputSchema if str(x.dataType)=="IntegerType" and x.name in subset)
    for x in julCol:
      self.inputDf = (self.inputDf.withColumn(x.name,col(x.name).cast("string"))
                                 .withColumn(x.name+"_year",
                                             when((length(col(x.name))==5) & (substring(col(x.name),1,2) <=50),concat(lit('20'),substring(col(x.name),1,2)))
                                             .when((length(col(x.name))==5) & (substring(col(x.name),1,2) >50),concat(lit('19'),substring(col(x.name),1,2)))
                                             .when(length(col(x.name))==7,substring(col(x.name),1,4))
                                             .otherwise(lit(0))
                                            )
                                 .withColumn(x.name+"_days",
                                             when(length(col(x.name))==5,substring(col(x.name),3,3).cast("int"))
                                             .when(length(col(x.name))==7,substring(col(x.name),5,3).cast("int"))
                                             .otherwise(lit(0))
                                            )
                                 .withColumn(x.name+"_ref_year",concat(col(x.name+"_year"),lit("-01"),lit("-01")).cast("date"))
                                 .withColumn(x.name+"_calendar",expr("date_add(" + x.name+"_ref_year"+","+ x.name+"_days)-1"))
                                 .drop(x.name, x.name+"_year",x.name+"_days",x.name+"_ref_year")
                                 .withColumnRenamed(x.name+"_calendar",x.name)
                                 
                     )
    return self.inputDf 
  
# Convert Calendar Date to Julian Date 
  def calendar_to_julian(self, subset):
    calCol = (x for x in self.inputSchema if ((str(x.dataType)=="DateType" or str(x.dataType)=="TimestampType") and x.name in subset))

    for x in calCol:
      self.inputDf = (self.inputDf.withColumn(x.name+"_ref_year", concat(year(col(x.name)).cast("string"),lit("-01"),lit("-01")))
                                  .withColumn(x.name+"_datediff", datediff(col(x.name),col(x.name+"_ref_year"))+1)
                                  .withColumn(x.name+"_julian", concat(substring(year(col(x.name)).cast("string"),3,2),col(x.name+"_datediff")).cast("int"))
                                  .drop(x.name,x.name+"_ref_year",x.name+"_datediff")
                                  .withColumnRenamed(x.name+"_julian",x.name)
                     )
    return self.inputDf

# Add a set of literal value columns to dataframe, pass as dictionary parameter  
  def addLitCols(self,colDict):
    for x in colDict.items():
      self.inputDf = self.inputDf.withColumn(x[0],lit(x[1]))
    return self.inputDf
