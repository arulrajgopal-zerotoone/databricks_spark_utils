from pyspark.sql.types import *
from pyspark.sql.functions import col

record_1 = [1,'A','arul','cricket']
record_2 = [2,'A','sekar','chess']
record_3 = [3,'A','kumar','tennis']
record_4 = [1,'B', 'ganesh','football']
record_5 = [2,'B','vinoth','volleyball']
record_6 = [3,'B','Ravi','hockey']


list = [record_1, record_2, record_3,record_4,record_5,record_6]

df_schema = StructType(fields=[StructField("sr_no", IntegerType(), False),
                               StructField("section", StringType(), False),
                                StructField("name", StringType(), True),
                               StructField("fav_game", StringType(), True)    
])


df = spark.createDataFrame(list, df_schema)

#Note: - the below methods not documented properly
#df.distinct()
#df.groupby()
#windowspec & using rownumber
#dropduplicates()


