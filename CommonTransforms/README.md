# CommonTransforms

CommonTransforms is a Python class that uses PySpark libraries to apply common transformations to a Spark dataframe. 

## Getting Started
if using Databricks, use %run magic command to include this notebook.
```python
%run "/<notebook path in workspace>/CommonTransforms"
```
Then instantiate the class by passing yor input dataframe
```python
ct = CommonTransforms(df)
```

## Function Reference
CommonTransforms supports the following functions:

### 1. trim
Removes leading and trailing spaces from all string columns in the dataframe

  * **Parameters:** None  

  * **Usage:**
```python
df = ct.trim()
```
### 2. replaceNull
Replace null values in dataframe with a default value. The default value is applied to all columns or a subset of columns passed as a list. The default value could be numeric, string, date, timestamp, boolean or dictionary object. When dictionary object is passed, custom default values can be applied to specified columns. The default value is only applied to the columns of same data type. For e.g. if the default value is a string only the string columns which are null are replaced and the numeric columns are untouched.

  * **Parameters:**
    * value - int, long, float, string, bool date, timestamp or dict. Value to replace null values with. If the value is a dict, then subset is ignored and value must be a mapping from column name (string) to replacement value. The replacement value must be an int, long, float, boolean, or string.
    * subset – optional list of column names to consider. Columns specified in subset that do not have matching data type are ignored. For example, if value is a string, and subset contains a non-string column, then the non-string column is simply ignored.

  * **Usage:**
```python
df = ct.replaceNull(0)
```
```python
df = ct.replaceNull("NA")
```

```python
df = ct.replaceNull("1900-01-01T00:00:00","start_datetime")
df = ct.replaceNull("9999-12-31T23:59:59","end_datetime")
```

```python
df = ct.ct.replaceNull({"passenger_count":1,"store_and_fwd_flag":"N","tip_amount":0})
```
### 3. deDuplicate
Delete duplicate records from dataframe with option to consider a subset of key columns.

  * **Parameters:**
    * subset - optional list of column names to consider.
    
  * **Usage:**
```python
df = ct.deDuplicate()
```

```python
df = ct.deDuplicate(["col1","col2"])
```
### 4. utc_to_local
Convert all or a subset of timestamp columns from UTC to timestamp in local timezone

  * **Parameters:**
    * localTimeZone - your local timezone specified as Country/City. Here is the list of [timezones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).
    * subset - optional list of column names to consider.
    
  * **Usage:**
```python
df = ct.utc_to_local("Australia/Sydney")
```
```python
df = ct.utc_to_local("Australia/Sydney",["pickup_datetime","dropoff_datetime"])
```
### 5. local_to_utc
Convert all or a subset of timestamp columns from local timezone to UTC 

  * **Parameters:**
    * localTimeZone - your local timezone specified as Country/City. Here is the list of [timezones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).
    * subset - optional list of column names to consider.
    
  * **Usage:**
```python
df = ct.local_to_utc("Australia/Sydney")
```
```python
df = ct.utc_to_local("Australia/Sydney",["recorded_datetime"])
```
### 6. changeTimezone
Converts all or selected timestamps in dataframe from one timezone to another.

  * **Parameters:**
    * fromTimezone - specified as Country/City. Here is the list of [timezones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).
    * toTimezone -  specified as Country/City

  * **Usage:**
```python
df = ct.changeTimezone("Australia/Sydney","America/New_York")
```
### 7. dropSysColumns
Drop columns that are either system or non-business from dataframe

  * **Parameters:**
   * columns - list of columns to be dropped
   
  * **Usage:**
```python
df = ct.dropSysColumns(["col1","col2"])
```
### 8. addChecksumCol
Create a checksum column using all columns of the dataframe

  * **Parameters:**
   * colName - Name of the new checksum column
   
  * **Usage:**
```python
df = ct.addChecksumCol("checksum")
```
### 9. julian_to_calendar
Converts a 5-digit or 7-digit Julian date to a calendar date

  * **Parameters:**
   * subset - a mandatory list of columns that contain a Julian date value
   
  * **Usage:**
```python
df = df.withColumn("sys_date1",lit(20275)) #Date in Julian Format
df = ct.julian_to_calendar("sys_date1")
# Output=2020-10-01
```

### 10. calendar_to_julian
Converts a calendar date to 5-digit julian date

  * **Parameters:**
   * subset - a mandatory list of columns that contain a date
  * **Usage:**

```python
df = df.withColumn("sys_date2",lit("2020-10-01").cast("date")) #Date in Gregorian Format
df = ct.calendar_to_julian("sys_date2")
# Output=20275
```
