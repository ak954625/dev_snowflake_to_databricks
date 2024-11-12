# Databricks notebook source
dbutils.widgets.text("Environment", "", "Environment")
env = dbutils.widgets.get("Environment")

# COMMAND ----------

# MAGIC %run "/Workspace/Get_Credentials"

# COMMAND ----------

from datetime import datetime
from pytz import timezone
from pyspark.sql.functions import lit

# Get the current timestamp in CST and format it
jobStartTime = spark.sql("select from_utc_timestamp(current_timestamp(), 'CST') as jobStartTime").first()[0]
batchId = jobStartTime.strftime('%Y%m%d%H%M%S')

# Set the timezone to US/Central
est = timezone('US/Central')

# Retrieve active configurations from the <catalog_name_where_delta_tables_are_created>.config_snowflake_ingestion table
df = spark.sql("select * from <catalog_name_where_delta_tables_are_created>.config_snowflake_ingestion where isActive=True order by sourceName")
rows = df.collect()

# Iterate over each configuration row
for row in rows:
  # Get the current time in the specified timezone
  startTime = datetime.now(est).strftime("%Y-%m-%d %H:%M:%S")
  
  # Get environment-specific configurations
  read_config, write_config, query_params = env_config(env)
  
  # Replace schema placeholder in the read configuration
  read_config['sfSchema'] = read_config['sfSchema'].replace("<Schema>", row['schema'])
  
  # Check if the extraction mode is incremental
  if(row['extractionMode'] == 'incremental'):
    # Replace placeholders in the source query for incremental extraction
    query = row['sourceQuery'].replace("<IncrementalColumn>", row['incrementalColumn']).replace("<IncrementalStartTime>", str(row['incrementalStartTime'])).replace("<JobStartTime>", str(jobStartTime))
    print("\nLoading incremental data from "+row['schema']+"."+row['sourceName']+ " into <catalog_name_where_delta_tables_are_created>.bronze_" + row['sourceName'])
    print("Query : "+query)
    # Load data from Snowflake using the query
    tbl = spark.read.format("snowflake").options(**read_config).option("query", query).load()
  else:
    # Use the source query for full extraction
    query=row['sourceQuery']
    print("\nLoading full data from "+row['schema']+"."+row['sourceName']+ " into <catalog_name_where_delta_tables_are_created>.bronze_" + row['sourceName'])
    print("Query : "+query)
    # Load data from Snowflake using the query
    tbl = spark.read.format("snowflake").options(**read_config).option("query", query).load()
  
  # Add ETL_CREATED_DTTM column to the dataframe
  tbl = tbl.withColumn("ETL_CREATED_DTTM", lit(jobStartTime).cast("timestamp"))
  
  # Drop the existing table if it exists
  spark.sql(f"DROP TABLE IF EXISTS <catalog_name_where_delta_tables_are_created>.bronze_{row['sourceName']}")
  
  # Write the dataframe to a Delta table
  tbl.write.format("delta").mode("overwrite").saveAsTable("<catalog_name_where_delta_tables_are_created>.bronze_"+row['sourceName'])
  
  # Get the count of records in the newly created table
  sourcecount = spark.sql(f"select count(1) as count from <catalog_name_where_delta_tables_are_created>.bronze_{row['sourceName']}").collect()[0]['count']
  
  # Optimize the Delta table (To be replaced with liquid clustering if required in next phases)
  print("Optimizing table <catalog_name_where_delta_tables_are_created>.bronze_" + row['sourceName'])
  spark.sql(f"OPTIMIZE <catalog_name_where_delta_tables_are_created>.bronze_{row['sourceName']}")
  
  # Get the end time of the process
  endTime = datetime.now(est).strftime("%Y-%m-%d %H:%M:%S")
  
  # Escape single quotes in the query
  escaped_query = query.replace("'", "\\'")
  
  # Log the ingestion details into the log table
  spark.sql(f"""INSERT INTO <catalog_name_where_delta_tables_are_created>.log_snowflake_ingestion (batchId, sourceName, startTime, endTime, sourceCount, queryUsed) VALUES ({batchId}, '{row['sourceName']}', '{startTime}', '{endTime}', {sourcecount}, "{escaped_query}")""")
  
  # Print completion message
  print("Completed writing to <catalog_name_where_delta_tables_are_created>.bronze_" + row['sourceName'])

# COMMAND ----------

# Convert job start time to ISO format string
jobStartTime_str = jobStartTime.isoformat()

# Set the job start time as a task value for downstream tasks
dbutils.jobs.taskValues.set(key="jobStartTime", value=jobStartTime_str)
dbutils.jobs.taskValues.set(key="environment", value=env)
