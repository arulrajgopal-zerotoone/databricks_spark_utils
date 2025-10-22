from pyspark.sql.functions import col, lit, concat, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType 
from datetime import datetime

catalog_name = "my_catalog"
schema_name = "my_schema"
catalog_schema_name = catalog_name+"."+schema_name

tbl_lst_df = spark.sql(f"show tables from {catalog_schema_name}")\
			.withColumn("Table",concat(lit(catalog_name),lit("."),col("database"),lit("."),col("tablename")))\
			.select("Table")

def get_version (tbl_name):
	return spark.sql(f"desc history {tbl_name}").orderBy(desc("version")).select("version").limit(1).collect()[0][0]

def get_count (tbl_name):
	return spark.sql(f"select count(*) from {tbl_name}").collect()[0][0]

final_data_lst =[]
for tbl in tbl_lst_df.rdd.collect():
	table_name = tbl[0]
	version = get_version(table_name)
	row_count = get_count(table_name)
	lst = [table_name, version, row_count]
	final_data_lst.append(lst)

columns = ["TableName","Version","RowCount"]
spark.createDataFrame(final_data_lst, columns).display()


