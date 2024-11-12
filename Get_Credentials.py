# Databricks notebook source
def get_keyvault_secret(scope_name, key_name):
  try:
      return dbutils.secrets.get(scope=scope_name, key=key_name)
  except Exception as e:
      print(f"Failed to access the Key Vault: {str(e)}")
      raise

# COMMAND ----------

def env_config(env):
    if env == "DEV":
        readconfig = {
            "sfUrl": "<snowflake_azure_url>",
            "sfUser": "<snowflake_dev_service_account_username>",
            "sfPassword": get_keyvault_secret(scope_name="<scope_name>", key_name="<dev_key_name>"),
            "sfDatabase": "<snowflake_dev_DB_name>",
            "sfSchema": "<snowflake_Schema>",
            "sfWarehouse": "<snowflake_warehouse_name>",
            "sfRole": "<snowflake_dev_role>",
            "TIMESTAMP_TYPE_MAPPING": "TIMESTAMP_LTZ"
        } 
        writeconfig = {
            "sfUrl": "<snowflake_azure_url>",
            "sfUser": "<snowflake_dev_service_account_username>",
            "sfPassword": get_keyvault_secret(scope_name="<scope_name>", key_name="<dev_key_name>"),
            "sfDatabase": "<snowflake_dev_DB_name>",
            "sfSchema": "<snowflake_Schema>",
            "sfWarehouse": "<snowflake_warehouse_name>",
            "sfRole": "<snowflake_dev_role>",
            "TIMESTAMP_TYPE_MAPPING": "TIMESTAMP_LTZ"
        }
        query_params = {
            "user": "<dev_username>",
            "password": get_keyvault_secret(scope_name="<scope_name>", key_name="<dev_key_name>"),
            "account": "<azure_account>",
            "warehouse": "<snowflake_warehouse_name>",
            "database": "<DEV_DB_name>",
            "schema": "<schema_name>",
            "role": "<snowflake_dev_role>"
        } 
    elif env == "QA":
        readconfig = {
            "sfUrl": "<snowflake_azure_url>",
            "sfUser": "<dev_username>",
            "sfPassword": get_keyvault_secret(scope_name="<scope_name>", key_name="<dev_key_name>"),
            "sfDatabase": "<snowflake_QA_DB_name>",
            "sfSchema": "<Schema>",
            "sfWarehouse": "<snowflake_warehouse_name>",
            "sfRole": "<snowflake_dev_role>",
            "TIMESTAMP_TYPE_MAPPING": "TIMESTAMP_LTZ"
        } 
        writeconfig = {
            "sfUrl": "<snowflake_azure_url>",
            "sfUser": "<snowflake_QA_service_account_username>",
            "sfPassword": get_keyvault_secret(scope_name="<scope_name>", key_name="<QA_key_name>"),
            "sfDatabase": "<snowflake_QA_DB_name>",
            "sfSchema": "<snowflake_QA_Schema_name>",
            "sfWarehouse": "<snowflake_warehouse_name>",
            "sfRole": "<snowflake_QA_service_account_username>_ROLE",
            "TIMESTAMP_TYPE_MAPPING": "TIMESTAMP_LTZ"
        }    
        query_params = {
            "user": "<snowflake_QA_service_account_username>",
            "password": get_keyvault_secret(scope_name="<scope_name>", key_name="<QA_key_name>"),
            "account": "<azure_account>",
            "warehouse": "<snowflake_warehouse_name>",
            "database": "<snowflake_QA_DB_name>",
            "schema": "<snowflake_QA_Schema_name>",
            "role": "<snowflake_QA_service_account_username>_ROLE"
        }  
    elif env == "PROD":
        readconfig = {
            "sfUrl": "<snowflake_azure_url>",
            "sfUser": "<snowflake_PROD_service_account_username>",
            "sfPassword": get_keyvault_secret(scope_name="<scope_name>", key_name="<PROD_key_name>"),
            "sfDatabase": "<PROD_DB_NAME>",
            "sfSchema": "<Schema>",
            "sfWarehouse": "<snowflake_warehouse_name>",
            "sfRole": "<snowflake_PROD_service_account_username>_ROLE",
            "TIMESTAMP_TYPE_MAPPING": "TIMESTAMP_LTZ"
        } 
        writeconfig = {
            "sfUrl": "<snowflake_azure_url>",
            "sfUser": "<snowflake_PROD_service_account_username>",
            "sfPassword": get_keyvault_secret(scope_name="<scope_name>", key_name="<PROD_key_name>"),
            "sfDatabase": "<PROD_DB_NAME>",
            "sfSchema": "<snowflake_QA_Schema_name>",
            "sfWarehouse": "<snowflake_warehouse_name>",
            "sfRole": "<snowflake_PROD_service_account_username>_ROLE",
            "TIMESTAMP_TYPE_MAPPING": "TIMESTAMP_LTZ"
        }        
        query_params = {
            "user": "<snowflake_PROD_service_account_username>",
            "password": get_keyvault_secret(scope_name="<scope_name>", key_name="<PROD_key_name>"),
            "account": "<azure_account>",
            "warehouse": "<snowflake_warehouse_name>",
            "database": "<PROD_DB_NAME>",
            "schema": "<snowflake_QA_Schema_name>",
            "role": "<snowflake_PROD_service_account_username>_ROLE"
        }     

    return readconfig, writeconfig, query_params


# COMMAND ----------

def execute_snowflake_query(query_params, query):
    import snowflake.connector

    conn = snowflake.connector.connect(**query_params)
    cur = conn.cursor()

    try:
        cur.execute(query)
        results = cur.fetchall()
        for row in results:
            print(row)
    finally:
        cur.close()
        conn.close()
