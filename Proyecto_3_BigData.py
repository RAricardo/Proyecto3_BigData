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

regex_tokenizer= RegexTokenizer(inputCol='content', outputCol='words',pattern='[^a-zA-Z0-9]+').setMinTokenLength(2)
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

# COMMAND ----------

from pyspark.ml.feature import HashingTF, IDF

# COMMAND ----------

hashing_tf = HashingTF(inputCol='words', outputCol='rawFeatures')

# COMMAND ----------

featurized_data = hashing_tf.transform(final_df)
featurized_data.cache

# COMMAND ----------

featurized_data.show()

# COMMAND ----------

idf = IDF(inputCol='rawFeatures', outputCol='features')

# COMMAND ----------

idf_model = idf.fit(featurized_data)

# COMMAND ----------

rescaled_data = idf_model.transform(featurized_data)

# COMMAND ----------

ii_df = rescaled_data.select("id","title","words","features")

# COMMAND ----------

ii_df.first()

# COMMAND ----------


