# Databricks notebook source
# DBTITLE 1,loading necessary libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from delta import DeltaTable

# COMMAND ----------

# DBTITLE 1,establishing adls connectivity
spark.conf.set(
    "fs.azure.account.key.arulrajstorageaccount.blob.core.windows.net",
    "<account-key>"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore;
# MAGIC CREATE DATABASE IF NOT EXISTS SCD_DEMO;
# MAGIC USE DATABASE SCD_DEMO;

# COMMAND ----------

# DBTITLE 1,reading full file and load it into table
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("emp_city", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("designation", StringType(), True)
])

emp_full_df = spark.read\
        .format("csv")\
        .schema(schema)\
        .option("Header",True)\
        .option("sep", "~")\
        .load("wasbs://private@arulrajstorageaccount.blob.core.windows.net/sample_employee/scd_implementations/emp_details.csv")


emp_full_df.write.mode("overwrite").saveAsTable("hive_metastore.scd_demo.emp_details")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.scd_demo.emp_details

# COMMAND ----------

# DBTITLE 1,reading delta file
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("emp_city", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("designation", StringType(), True)
])

emp_delta_df = spark.read\
        .format("csv")\
        .schema(schema)\
        .option("Header",True)\
        .option("sep", "~")\
        .load("wasbs://private@arulrajstorageaccount.blob.core.windows.net/sample_employee/scd_implementations/emp_details_delta.csv")



dt = DeltaTable.forName(spark, "hive_metastore.scd_demo.emp_details")

dt.alias("tgt").merge(emp_delta_df.alias("src"), "tgt.id=src.id")\
                .whenNotMatchedInsertAll()\
                .whenMatchedUpdate(set = {
                    "name": "src.name",
                    "emp_city": "src.emp_city",
                    "salary": "src.salary",
                    "designation": "src.designation"
                })\
                .execute()

emp_full_df.write.format("delta").mode("overwrite").saveAsTable("default.emp_details")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.scd_demo.emp_details

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


