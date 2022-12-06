# Databricks notebook source
diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")

diamonds.count()

# COMMAND ----------

diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")

diamonds.count()

# COMMAND ----------

diamondsICareAbout = diamonds.where(diamonds.price >10000)
diamondsICareAbout.show()

# COMMAND ----------

oldTable = spark.read.table("hive_metastore.default.theGoodDiamonds")

# COMMAND ----------

completeList = diamondsICareAbout.union(oldTable)

# COMMAND ----------

completeList.write.mode("overwrite").saveAsTable("theGoodDiamonds")
