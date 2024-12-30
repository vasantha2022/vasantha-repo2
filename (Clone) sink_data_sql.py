# Databricks notebook source
import os,logging,ast
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import * 
import traceback
import json
from datetime import datetime
from pyspark.sql import SparkSession
import ast
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("tenant_id", "")
dbutils.widgets.text("container_name", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("log_parent_run_id", "")
dbutils.widgets.text("file_pattern","")
dbutils.widgets.text("master_config_file","")
dbutils.widgets.text("date_path","")
dbutils.widgets.text("source_type","")
dbutils.widgets.text("list_stages","")
dbutils.widgets.text("data_source","")
dbutils.widgets.text("source_table_name","")
dbutils.widgets.text("output_table_name","")
dbutils.widgets.text("stage_context", "", "stage_context")

tenant_id = dbutils.widgets.get("tenant_id");
storage_account = dbutils.widgets.get("storage_account");
container_name = dbutils.widgets.get("container_name");
log_parent_run_id = dbutils.widgets.get("log_parent_run_id")
source_file_pattern=dbutils.widgets.get("file_pattern")
master_config_file = dbutils.widgets.get("master_config_file")
date_path = dbutils.widgets.get("date_path")
source_type = dbutils.widgets.get("source_type")
list_stages = dbutils.widgets.get("list_stages")
data_source=dbutils.widgets.get("data_source")
source_table_name=dbutils.widgets.get("source_table_name")
output_table_name=dbutils.widgets.get("output_table_name")
stage_context = dbutils.widgets.get('stage_context').strip()
# Convert file extension to lower for all files to avoid conflicts

# COMMAND ----------

# Define a function to log error messages to a file status log.
fail_cell=False
error_list=[]
def log_message(cell_name, error_reason, error_description):
    global fail_cell
    # Remove any single or double quotes from the error reason to prevent formatting issues.
    e = re.sub(r"['\"]", "", str(error_reason))
    
    # Remove any single or double quotes from the error description for the same reason.
    ed = re.sub(r"['\"]", "", str(error_description))
    
    # Prepare the update dictionary with formatted error information.
    update_dict = {
        "error_code": "ADB_ERR",
        "error_stage": file_status_log_stage,
        "error_reason": f"{cell_name} : {e}",
        "error_description": ed,
        "execution_end_time": str(datetime.utcnow()),
        "execution_end_time": str(datetime.utcnow())
    }
    
    # Prepare the stage dictionary noting that the stage has not been executed successfully.
    stage_dict={
        "stage_executed":False, 
        "stage_status":False, 
        "stage_start_time": stage_start_time, 
        "stage_end_time": str(datetime.utcnow())
    }

    # Assemble all necessary parameters for logging the error.
    file_status_log_args = {
        "schema_name": file_status_log_schema,
        "table_name": file_status_log_table,
        "log_parent_run_id": log_parent_run_id,
        "data_source": data_source.lower(),
        "update_dict": f"{update_dict}",
        "stage_dict": f"{stage_dict}",
        "stage": file_status_log_stage
    }
    
    # Execute the logging notebook with the prepared arguments.
    dbutils.notebook.run(file_status_log_notebook_path, 0, file_status_log_args)
    
    # Print the error message to the console for immediate feedback.
    print(f"Error in function {cell_name} : {error_reason}")
    
    # Set the fail_cell variable to True to indicate a failure has occurred.
    fail_cell = True
    error_list.append(f"{cell_name} : {e}")

# COMMAND ----------

# To read parameters from the master config file
fp_master_config = f"/dbfs/mnt/{os.path.join(storage_account,container_name, master_config_file)}"
f= open(fp_master_config)
master_config_data= json.load(f)

adls_file_path=os.path.join("dbfs:/mnt/", storage_account, container_name)

config_folder_name = master_config_data["folder_path"]["fp_config"]
temp_folder_name = master_config_data["folder_path"]["fp_temp"]
suffix_metadata_template = master_config_data['file_meta']['suffix_metadata_template']
fp_metadata= master_config_data['folder_path']['fp_metadata']
#table_gold = f"{data_source}{suffix_gold_table}"

metadata_template_name = f"{tenant_id}_{data_source}{suffix_metadata_template}"
metadata_file_path = os.path.join(adls_file_path,fp_metadata)
metadata_schema_mapping_file = os.path.join("/",metadata_file_path, metadata_template_name).replace(':','')

fp_delta_output=master_config_data['folder_path']['fp_delta_output']
delta_tbl_schema = master_config_data['delta_table_details']['schema']
sql_tbl_schema = master_config_data['common_config']['sql_tbl_schema']
sql_sink_tbl_name = master_config_data['common_config']['sql_sink_tbl_name']
sql_table_gold = f"{sql_tbl_schema}.tbl_{data_source}_sink"
sql_sink_log = f"{sql_tbl_schema}.{sql_sink_tbl_name}"
#adb_sink_log = f"{data_source}{suffix_gold_table}_sink_log"
folder_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath("__file__"))))
db_utils_notebook_path=f"{folder_path}/ada_workflow/utils/db_utils"

adls_file_path=os.path.join("dbfs:/mnt/", storage_account, container_name)
data_layer = "sink"
data_layer_sink_log = "data_delivery"

out_file_path = os.path.join(adls_file_path,fp_delta_output,source_type,data_source,data_layer)
delta_file_path = os.path.join(adls_file_path,fp_delta_output,source_type,data_source,data_layer_sink_log, 'gold_transform')
sink_log_path = os.path.join(adls_file_path,fp_delta_output,source_type,data_source,data_layer_sink_log,'sink_log')
print(sink_log_path)

#read stage wise config params
fp_cfg_file_path = f"{tenant_id}_ada_cfg_params.json"
fp_config_params = f"/dbfs/mnt/{os.path.join(storage_account,container_name,config_folder_name,fp_cfg_file_path)}"
f1= open(fp_config_params)
config_data= json.load(f1)

# Iterate through the list of dictionaries
for item in config_data:
    if data_source in item:
        data_delivery_node = item[data_source]["data_delivery"]
        copy_mode= data_delivery_node["write_mode"]

#read sql tables config
fp_sql_tables_config = f"{tenant_id}_sql_tables_cfg.json"
fp_sql_config = f"/dbfs/mnt/{os.path.join(storage_account,container_name,config_folder_name,fp_sql_tables_config)}"
f2= open(fp_sql_config)
sql_config_data= json.load(f2)

# FIle status Log Variables
file_status_log_schema = master_config_data['delta_table_details']['schema']
file_status_log_table = master_config_data['common_config']['tbl_file_status_log']
file_status_log_stage="data_delivery"

folder_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath("__file__"))))
file_status_log_notebook_path=f"{folder_path}/ada_workflow/utils/file_status_error_log_module"
stage_start_time=str(datetime.utcnow())

#reading schema mapping sheet for target to source col mapping
schema_mapping_df = pd.read_excel(metadata_schema_mapping_file, sheet_name="schema_mapping", header=0)
schema_mapping_df = schema_mapping_df.fillna('')
schema_mapping_spark_df = spark.createDataFrame(schema_mapping_df)
#display(schema_mapping_spark_df)

# COMMAND ----------

#function to generate query using columns dict
cell_name = "define generate query function"
try:
    if not fail_cell: 
        #function to generate query using columns dict
        def generate_create_table_query(table_name, columns):
            columns_def = ",\n ".join([f"{col} {dtype}" for col, dtype in columns.items()])
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns_def}
            );
            """
            return create_table_query

        #function to replace column names with space including nested columns
        def replace_space_in_schema(schema):
            new_fields = []
            for field in schema.fields:
                new_name = field.name.replace(' ', '_')
                if isinstance(field.dataType, StructType):
                    new_struct = replace_space_in_schema(field.dataType)
                    new_fields.append(StructField(new_name, new_struct, field.nullable))
                else:
                    new_fields.append(StructField(new_name, field.dataType, field.nullable))
            return StructType(new_fields)
        
        def apply_formats(df, format_dict, dtype_dict):
            for column_name, format_spec in format_dict.items():
                if column_name in df.columns:
                    psql_dtype = dtype_dict.get(column_name)
                    # If the data type is text and a format is specified, format the timestamp and save as text
                    if psql_dtype == 'text' and format_spec:
                        print(f"Formatting column '{column_name}' as '{format_spec}'")
                        df = df.withColumn(column_name, date_format(col(column_name), format_spec).cast('string'))
                    elif psql_dtype == 'timestamp':
                        print(f"Casting column '{column_name}' to timestamp")
                        df = df.withColumn(column_name, col(column_name).cast('timestamp'))
                    else:
                        print(psql_dtype)
            return df
except Exception as e:
    log_message(cell_name, e, traceback.format_exc())

# COMMAND ----------

# MAGIC %run ../utils/metadata_util

# COMMAND ----------

utilsObj = Utils()

# COMMAND ----------

cell_name = "define generate query function"
try:
    if not fail_cell:  
        sheet_names = pd.ExcelFile(metadata_schema_mapping_file).sheet_names
        dataframes = {}
        for sheet_name in sheet_names:
            if sheet_name.lower() in ['centralized_logs', 'data_delivery']:
                excel_df = pd.read_excel(metadata_schema_mapping_file, sheet_name=sheet_name, header=0)
                excel_df = excel_df.fillna('')
                spark_df = spark.createDataFrame(excel_df)
                
                if sheet_name.lower() == 'centralized_logs': attribute_name = 'column_names'
                elif sheet_name.lower() == 'data_delivery': attribute_name = 'sink_columns'
                
                if attribute_name and attribute_name in spark_df.columns:
                    spark_df = utilsObj.special_chars_removal_from_col_value(spark_df,attribute_name=attribute_name,spark_flag=True,regex_pattern='[^0-9a-zA-Z,\w\s_-]')
                spark_df = spark_df.select(*[col(column).alias(column.lower()) for column in spark_df.columns])
                dataframes[sheet_name] = spark_df

        centralized_logs_df = dataframes.get('centralized_logs')
        sink_df = dataframes.get('data_delivery')
        # Process centralized_logs_df if it exists
        if centralized_logs_df:
            for row in centralized_logs_df.collect():
                if row["table_type"] == 'sink_error':
                    row_identifier_col_list = row["column_names"].strip().split(',')
                    row_identifier_col_list = [c.lower() for c in row_identifier_col_list]
                    sink_columns = row["ada_columns"].strip().split(',')
        format_spec_dict = {row['sink_columns']: row['format'] for row in sink_df.collect() if row['format']}
        format_spec_dict = {k.lower(): v for k, v in format_spec_dict.items()}
        print(format_spec_dict)
        out_dict = {row['sink_columns'].lower(): row['psql_dtypes'].lower() for row in sink_df.collect() if row['psql_dtypes']}
        #print(out_dict)
        tgt_col = [row['sink_columns'].strip().lower() for row in sink_df.collect()]
        Src_col= spark.sql(f"select * from ada.{source_table_name}").schema.names
        common_columns = [col for col in Src_col if col in tgt_col]
except Exception as e:
    log_message(cell_name, e, traceback.format_exc())

# COMMAND ----------

cell_name = "gold_table date columns formatting writing to delta path"
try:
    if not fail_cell:
        tbl_gold = spark.sql("SELECT * FROM {0}.{1} WHERE log_parent_run_id = '{2}'".format(delta_tbl_schema, source_table_name, log_parent_run_id))
        
        for field in tbl_gold.schema.fields:
            if isinstance(field.dataType, DecimalType):
                tbl_gold = tbl_gold.withColumn(field.name, col(field.name).cast("float"))

        selected_columns = [f"`{col_name}`" for col_name in common_columns]
        sink_df = tbl_gold.select(selected_columns)
        #date format conversions
        sink_formats = apply_formats(sink_df, format_spec_dict,out_dict)
        sink_gold = utilsObj.special_chars_removal_from_col_name(sink_formats,spark_flag = True)
        display(sink_gold.limit(5))
        sink_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(delta_file_path)
except Exception as e:
    log_message(cell_name, e, traceback.format_exc())

# COMMAND ----------

cell_name = "replicating gold table to psql"
try:
    if not fail_cell: 
        db_utils_args={"master_config_file" : master_config_file,"tenant_id" : tenant_id,"data_source" : data_source,"container_name" : container_name,"storage_account" : storage_account,"sql_table_name" : sql_table_gold, "delta_table_path" : delta_file_path, "mode": copy_mode}

        copy_status = dbutils.notebook.run(db_utils_notebook_path, 0, db_utils_args)
        print(copy_status)
except Exception as e:
    log_message(cell_name, e, traceback.format_exc())

# COMMAND ----------

cell_name = "define delete_delta_table function"
try:
    if not fail_cell: 
        # function to delete delta table
        def delete_delta_table(table_path):
            """
            Deletes a Delta table at the specified path.
            """
            if DeltaTable.isDeltaTable(spark, table_path):
                # Delete the Delta table files and directory
                dbutils.fs.rm(table_path, recurse=True)
                print(f"Delta table has been deleted.")
            else:
                print(f"No Delta table found at this path.")
        delete_delta_table(delta_file_path)
except Exception as e:
    log_message(cell_name, e, traceback.format_exc())

# COMMAND ----------

schema_df = utilsObj.special_chars_removal_from_col_value(schema_mapping_spark_df,attribute_name="source_column",spark_flag=True,regex_pattern='[^0-9a-zA-Z,\w\s_-]')

schema_df = utilsObj.special_chars_removal_from_col_value(schema_df,attribute_name="target_column",spark_flag=True,regex_pattern='[^0-9a-zA-Z,\w\s_-]')

schema_df = schema_df.withColumn('source_column', lower(schema_df['source_column']))
schema_df = schema_df.withColumn('target_column', lower(schema_df['target_column']))

schema_mapping_dict = dict(schema_df
                           .filter(col("source_column").isin(row_identifier_col_list))
                           .dropDuplicates(["source_column"])
                           .rdd.map(lambda row: (row["source_column"], row["target_column"]))
                           .collect())

#Rename columns iteratively
renamed_data_df = tbl_gold
for target_col, source_col in schema_mapping_dict.items():
    renamed_data_df = tbl_gold.withColumnRenamed(source_col, target_col)

#Cast all columns to string type
renamed_data_df = renamed_data_df.select([col(column).cast('string').alias(column) for column in renamed_data_df.columns])

#Create a 'row_identifier' column that is a struct of the columns in row_identifier_col_list
df_sink_log = renamed_data_df.select(
    *[col(c) if c in renamed_data_df.columns else lit(" ").alias(c) for c in sink_columns],
    struct(*[col(c) if c in renamed_data_df.columns else lit(" ").alias(c) for c in row_identifier_col_list]).alias("row_identifier")
)
df_sink_log = df_sink_log.withColumn("data_source", lit(data_source)) \
                        .withColumn("log_creation_datetime", current_timestamp()) \
                        .withColumn("log_parent_run_id", lit(log_parent_run_id)) \
                        .withColumn("sink_type", lit("database")) \
                        .withColumn("sink_format", lit("csv")) \
                        .withColumn("sink_status", struct(lit(copy_status[0]).alias("delivery_status"),
                            lit('').alias("default_status"))) \
                        .withColumn("sink_response", struct(lit(copy_status[1]).alias("delivery_message"),
                            lit('').alias("default_message")))
display(df_sink_log.limit(5))

# COMMAND ----------

cell_name = "update sink_log schema"
try:
    if not fail_cell:
        schema = StructType([
            StructField("data_source", StringType(), True),
            StructField("source_file_name", StringType(), True),
            StructField("target_file_name", StringType(), True),
            StructField("ada_guid", StringType(), True),
            StructField("row_identifier", StructType([
                StructField("ada_guid", StringType(), True)
            ]), True),
            StructField("log_creation_datetime", TimestampType(), True),
            StructField("log_parent_run_id", StringType(), True),
            StructField("sink_type", StringType(), True),
            StructField("sink_format", StringType(), True),
            StructField("sink_status", StructType([
                StructField("default_status", StringType(), True),
                StructField("delivery_status", StringType(), True)
            ]), True),
            StructField("sink_response", StructType([
                StructField("default_message", StringType(), True),
                StructField("delivery_message", StringType(), True) 
            ]), True)
        ])

        for field in schema.fields:
            if field.name not in df_sink_log.columns:
                df_sink_log = df_sink_log.withColumn(field.name, lit('null').cast(field.dataType))

        new_schema = replace_space_in_schema(df_sink_log.schema)
        df_sink_log = spark.createDataFrame(df_sink_log.rdd, schema=new_schema)
except Exception as e:
    log_message(cell_name, e, traceback.format_exc())

# COMMAND ----------

#creating sink log delta table

if not os.path.exists(out_file_path):  
    dbutils.fs.mkdirs(out_file_path)  
deltaload_existsfileflag = 0   
print("out_file location :- ", out_file_path)
spark.sql("CREATE DATABASE IF NOT EXISTS {0} LOCATION '{1}'".format(delta_tbl_schema,out_file_path))

# writing to delta table if exists
if spark.catalog.tableExists(f"{delta_tbl_schema}.{output_table_name}"):
    spark.sql("DELETE FROM {0}.{1} WHERE log_parent_run_id = '{2}' AND data_source = '{3}'".format(delta_tbl_schema, output_table_name, log_parent_run_id, data_source))
else:
    spark.sql("CREATE TABLE IF NOT EXISTS {0}.{1} USING DELTA LOCATION '{2}'".format(delta_tbl_schema, output_table_name, out_file_path))
    print(f"The table {delta_tbl_schema}.{output_table_name} is created.")

df_sink_log.write.mode('append').format('delta').option("mergeSchema", "true").saveAsTable(f"{delta_tbl_schema}.{output_table_name}")
print(f"Data loaded in {delta_tbl_schema}.{output_table_name} successfully")

# COMMAND ----------

#writing sink_log errors to a delta file
cell_name = "writing sink_log errors to a delta file"
try:
    if not fail_cell:
        # file_name list for file status log.
        log_file_table=spark.sql(f"SELECT target_file_name FROM {file_status_log_schema}.{file_status_log_table} WHERE log_parent_run_id='{log_parent_run_id}' AND lower(data_source)='{data_source.lower()}' AND source_file_name not like '%index%' AND (error_code is NULL OR error_code='' OR error_stage='{file_status_log_stage}')").collect()

        log_file_list={row.target_file_name for row in log_file_table}

        #writing sink_log errors to a delta file
        if copy_status[0] == 'failed':
            df_sink_log.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(sink_log_path)
            print(f'sink log written to :{sink_log_path}')

            #create sql sink_log table using json config
            for table in sql_config_data['tables']:
                if table['name'] == sql_sink_log:
                    col_dict = table['field_list']
                        
            query= generate_create_table_query(sql_sink_log, col_dict)
            db_utils_args={"master_config_file" : master_config_file,"tenant_id" : tenant_id,"data_source" : data_source,"container_name" : container_name,"storage_account" : storage_account,"sql_query" : query}
            dbutils.notebook.run(db_utils_notebook_path, 0, db_utils_args)

            sql_query = f"""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = '{sql_tbl_schema}' AND tablename = '{sql_sink_tbl_name}') THEN DELETE FROM {sql_sink_log} WHERE log_parent_run_id = '{str(log_parent_run_id)}' AND data_source = '{str(data_source)}';
                END IF;
            END
            $$;
            """
            db_utils_args={"master_config_file" : master_config_file,"tenant_id" : tenant_id,"data_source" : data_source,"container_name" : container_name,"storage_account" : storage_account,"query" : sql_query}
            update_count= dbutils.notebook.run(db_utils_notebook_path, 0, db_utils_args)
            #replicating sink log table to psql

            db_utils_args={"master_config_file" : master_config_file,"tenant_id" : tenant_id,"data_source" : data_source,"container_name" : container_name,"storage_account" : storage_account,"sql_table_name" : sql_sink_log,"delta_table_path" : sink_log_path}

            status= dbutils.notebook.run(db_utils_notebook_path, 0, db_utils_args)
            print(status)
            delete_delta_table(sink_log_path)
            # Log false for all files in file status log.
            if log_file_list:
                log_file_list=list(log_file_list)
                update_dict={"error_code": "ADB_ERR","error_stage": file_status_log_stage,"error_reason": "data delivery failed." ,"error_description": "","execution_end_time": str(datetime.utcnow())}
                stage_dict={"stage_executed":True, "stage_status":False, "stage_start_time": stage_start_time, "stage_end_time": str(datetime.utcnow())}
                file_status_log_args={"schema_name":file_status_log_schema,"table_name":file_status_log_table,"log_parent_run_id":log_parent_run_id,"data_source":data_source.lower(),"update_dict":f"{update_dict}","stage_dict":f"{stage_dict}","stage":file_status_log_stage,"target_file_name":f"{log_file_list}"}
                dbutils.notebook.run(file_status_log_notebook_path, 0, file_status_log_args)

        else:
            # Log true for all files in file status log.
            if log_file_list:
                log_file_list=list(log_file_list)
                update_dict={"error_code": "","error_stage": "","error_reason": "" ,"error_description": "","execution_end_time": str(datetime.utcnow())}
                stage_dict={"stage_executed":True, "stage_status":True, "stage_start_time": stage_start_time, "stage_end_time": str(datetime.utcnow())}
                file_status_log_args={"schema_name":file_status_log_schema,"table_name":file_status_log_table,"log_parent_run_id":log_parent_run_id,"data_source":data_source.lower(),"update_dict":f"{update_dict}","stage_dict":f"{stage_dict}","stage":file_status_log_stage,"target_file_name":f"{log_file_list}"}
                dbutils.notebook.run(file_status_log_notebook_path, 0, file_status_log_args)

except Exception as e:
    log_message(cell_name, e, traceback.format_exc())

# COMMAND ----------

cell_name = "executing summary notebook from sink log"
try:
    if not fail_cell: 
        # Define the new values you want to pass to the target notebook
        params = {
            "container_name": container_name,
            "data_source": data_source,
            "date_path": date_path,
            "file_pattern": source_file_pattern,
            "list_stages": list_stages,
            "log_parent_run_id": log_parent_run_id,
            "master_config_file": master_config_file,
            "source_type": source_type,
            "storage_account": storage_account,
            "tenant_id": tenant_id,
            "stage_context":stage_context
        }

        # Run the centralized logger module
        folder_path = os.path.dirname(os.path.dirname(os.path.abspath("__file__")))
        centralized_notebook_path=f"{folder_path}/monitoring/generate_summary"
        result = dbutils.notebook.run(centralized_notebook_path, 600, params)
except Exception as e:
    log_message(cell_name, e, traceback.format_exc())

# COMMAND ----------

if fail_cell==True:
    raise Exception(error_list)

