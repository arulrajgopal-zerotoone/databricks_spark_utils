# Databricks notebook source
# DBTITLE 1,loading necessary libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import lit, current_timestamp, col
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



update_added_df = emp_full_df\
                    .withColumn("isActive",lit("Y"))\
                    .withColumn("updated_time",current_timestamp())

update_added_df.write.mode("overwrite").saveAsTable("hive_metastore.scd_demo.emp_details_scd_2")


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



# COMMAND ----------

emp_details_scd_2_df = spark.table("hive_metastore.scd_demo.emp_details_scd_2")

new_key_added_df = emp_delta_df.withColumn("new_key",col("id"))

joined_df = emp_details_scd_2_df.alias("LH").join(new_key_added_df.alias("RH"), col("LH.id")==col("RH.id"),"inner")\
                    .select("RH.*")\
                    .withColumn("new_key",lit(None))

delta_df = new_key_added_df.union(joined_df)\
                    .withColumn("isActive", lit("Y"))\
                    .withColumn("updated_time",current_timestamp())

dt = DeltaTable.forName(spark, "hive_metastore.scd_demo.emp_details_scd_2")

dt.alias("tgt").merge(delta_df.alias("src"), "tgt.id=src.new_key")\
                .whenNotMatchedInsertAll()\
                .whenMatchedUpdate(set = {
                    "updated_time": lit(current_timestamp()),
                    "isActive":lit("N")
                })\
                .execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.scd_demo.emp_details_scd_2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.scd_demo.emp_details_scd_2 where isActive = "Y"
