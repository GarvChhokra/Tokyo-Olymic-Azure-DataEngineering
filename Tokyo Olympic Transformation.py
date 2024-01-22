# Databricks notebook source
# Getting the key value pairs from Azure Key Vault
clientid = dbutils.secrets.get(scope = "key-vault-scope", key = "application-clientid")
directorytenantid = dbutils.secrets.get(scope = "key-vault-scope", key = "directory-tenantid")
secretkey = dbutils.secrets.get(scope = "key-vault-scope", key = "secretkey")

# COMMAND ----------

# Creating connection from Azure Data Lake Storage 2 to DataBricks

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": clientid,
"fs.azure.account.oauth2.client.secret": secretkey,
"fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directorytenantid}/oauth2/token"}

dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@tokyolympicdatastorage.dfs.core.windows.net", # first one is container name and after @ it is storage name
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic"

# COMMAND ----------

spark

# COMMAND ----------

atheletes = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/atheletes.csv")

# COMMAND ----------

atheletes.show()

# COMMAND ----------

atheletes = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolymic/raw-data/atheletes.csv")
coaches = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolymic/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolymic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolymic/raw-data/teams.csv")

# COMMAND ----------

atheletes.printSchema()

# COMMAND ----------

coaches.show()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

coaches.isEmpty()

# COMMAND ----------

entriesgender.show()

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

# Changing the data type to int for Female, male, and total colum
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DecimalType, StringType
entriesgender = entriesgender.withColumn("Female", col("Female").cast(IntegerType()))\
        .withColumn("Male", col("Male").cast(IntegerType()))\
        .withColumn("Total", col("Total").cast(IntegerType()))

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

# Find the countries with the highest number of gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("Team_Country", "Gold").show()

# COMMAND ----------

# Calculate the average number of entries by gender for each discipline
average_entries_by_gender = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
average_entries_by_gender.show()

# COMMAND ----------

# Write the data into the Data Lake transform folder
# atheletes.write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed-data/")
atheletes.repartition(3).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed-data/atheletes") # if we want to save 3 file meaning creating RDD
coaches.repartition(1).write.option("header", "true").csv("/mnt/tokyoolymic/transformed-data/coaches")
entriesgender.write.option("header", "true").csv("/mnt/tokyoolymic/transformed-data/entriesgender")
teams.write.option("header", "true").csv("/mnt/tokyoolymic/transformed-data/teams")
medals.write.option("header", "true").csv("/mnt/tokyoolymic/transformed-data/medals")

# COMMAND ----------


