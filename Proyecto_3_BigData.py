# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RS").getOrCreate()
df = spark.read.csv("/FileStore/tables/articles1.csv", inferSchema=True, header=True)
data2 = spark.read.csv("/FileStore/tables/articles2.csv", inferSchema=True, header=True)
data3 = spark.read.csv("/FileStore/tables/articles3.csv", inferSchema=True, header=True)

# COMMAND ----------

df = df.union(data2)

# COMMAND ----------

df = df.union(data3)

# COMMAND ----------

df.show()

# COMMAND ----------



# COMMAND ----------

df = df.select("id", "title", "content")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.ml.feature import Tokenizer, RegexTokenizer

# COMMAND ----------

from pyspark.sql.functions import col, udf

# COMMAND ----------

from pyspark.sql.types import IntegerType

# COMMAND ----------

tokenizer = Tokenizer(inputCol='content', outputCol = 'words')
regex_tokenizer = RegexTokenizer(inputCol='content', outputCol='words', pattern=r'[^a-zA-Z0-9]+')

# COMMAND ----------

count_tokens = udf(lambda words:len(words), IntegerType())

# COMMAND ----------

tokenized = tokenizer.transform(df)

# COMMAND ----------

tokenized.show()

# COMMAND ----------

tokenized.withColumn('tokens', count_tokens(col('words'))).show()

# COMMAND ----------

regex_tokenizer= RegexTokenizer(inputCol='content', outputCol='words',pattern='[^a-zA-Z1-9]+').setMinTokenLength(2)
df=df.fillna("unknown", subset=["content","title"])
rg_tokenized = regex_tokenizer.transform(df)

# COMMAND ----------

rg_tokenized.show()

# COMMAND ----------

rg_tokenized.withColumn('tokens', count_tokens(col('words'))).show()

# COMMAND ----------

from pyspark.ml.feature import StopWordsRemover

# COMMAND ----------

remover = StopWordsRemover(inputCol='words', outputCol='filtered')

# COMMAND ----------

remover.transform(rg_tokenized).show()

# COMMAND ----------

final_df = remover.transform(rg_tokenized).select(col('id'), col('title'), col('filtered').alias('words'))

# COMMAND ----------

final_df.show()
final_df.schema

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

wordcounts_DF = final_df.select("id", f.explode("words").alias("words")).groupBy("id", "words").count().groupBy("id").agg(f.collect_list(f.struct(f.col("words"), f.col("count"))).alias("words"))

# COMMAND ----------

wordcounts_DF = final_df.select("id", f.explode("words").alias("words")).groupBy("id","words").count().groupBy("words").agg(f.collect_list(f.struct(f.col("id"), f.col("count"))).alias("wordcounts"))

# COMMAND ----------

from pyspark.sql.functions import asc
indice_inverso = wordcounts_DF.orderBy("words").sort(asc("words"))

# COMMAND ----------

indice_inverso.dtypes

# COMMAND ----------

indice_inverso.show(1000000)

# COMMAND ----------

dbutils.widgets.text("palabra","Inserte palabra"," ")

# COMMAND ----------

word_df = indice_inverso.filter(indice_inverso['words'] == dbutils.widgets.get("palabra"))
row = word_df.collect()
id_counts =row[0].wordcounts
id_counts = sorted(id_counts, key=lambda x: x[1])
row_id_array = list(reversed(id_counts))[:5]
for x in range (0,5):
  print(final_df.filter(final_df['id'] == row_id_array[x][0]).collect()[0].title)

# COMMAND ----------


