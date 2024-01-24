# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

# function arranges the partition by column to the end of the column_list
def re_arrange_partition_column(input_df, partition_column):
    #empty list
    column_list = []
    #loops through all column names that do not equal value of partition_column
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            #appends results to the column_list 
            column_list.append(column_name)
    #adds partition_column to the end of the column_list after 4 loop has completed
    column_list.append(partition_column)
    #creates var output_df and selects the updated coloumn_list with the column used to create the partition
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

# function overwrites and or appends the current partition
def overwrite_partition(input_df, db_name, table_name, partition_column):
    # var output_df contains the reordered/rearranged column list used to write the partition
    output_df = re_arrange_partition_column(input_df, partition_column)
    #sets the partitionOverwriteMode to "dynamic"
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # if the table exists, insertInto the table and or overwrite the data that currently exists
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        #spark expects the last column in the list to be the partitioned column
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    # else overwite data in the partition by partitionColumn and save
    else:
        output_df.write.mode("overwrite").partitionBy(f"{partition_column}").format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# takes df and column_name, goes through df, pulls out distinct values and collects them in a row_list
def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name) \
                          .distinct() \
                          .collect()
    # from row_list, get column_values, put into column_value_list and return 
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list

# COMMAND ----------



# COMMAND ----------

# merge delta lake table data for incremental load scenarios
def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        # if table exists, merge (costly operation)
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            # (partitionPruning) use race_id as a column to find the partition
            merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    # else: create table if it does not exist, overwrite the table and populate the data
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")