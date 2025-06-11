# Databricks notebook source
# MAGIC %md
# MAGIC Declare Variables

# COMMAND ----------

# Cell 1
dbutils.widgets.text("catalog", "dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.conf.set("sql.catalog", catalog) #Delta Table schema

# COMMAND ----------

# MAGIC %md
# MAGIC Create dataframe from the samples.nyctaxi.trips table

# COMMAND ----------

nyctaxi_df = spark.sql("select * from samples.nyctaxi.trips")

# COMMAND ----------

display(nyctaxi_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Create the enhanced NYC data frame

# COMMAND ----------

from pyspark.sql.functions import col,when, dayofweek, date_format, hour,unix_timestamp, round, dayofmonth, lit

nyctaxidf_prep = nyctaxi_df.withColumn('pickupDate', col('tpep_pickup_datetime').cast('date'))\
                            .withColumn("weekDay", dayofweek(col("tpep_pickup_datetime")))\
                            .withColumn("weekDayName", date_format(col("tpep_pickup_datetime"), "EEEE"))\
                            .withColumn("dayofMonth", dayofweek(col("tpep_pickup_datetime")))\
                            .withColumn("pickupHour", hour(col("tpep_pickup_datetime")))\
                            .withColumn("tripDuration", (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime")))/60)\
                            .withColumn("timeBins", when((col("pickupHour") >=7) & (col("pickupHour")<=10) ,"MorningRush")\
                            .when((col("pickupHour") >=11) & (col("pickupHour")<=15) ,"Afternoon")\
                            .when((col("pickupHour") >=16) & (col("pickupHour")<=19) ,"EveningRush")\
                            .when((col("pickupHour") <=6) | (col("pickupHour")>=20) ,"Night"))\
                            .filter("""fare_amount > 0 AND fare_amount < 100 and trip_distance > 0 AND trip_distance < 100 """)

# COMMAND ----------

display(nyctaxidf_prep.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Write the data to the nyctaxi delta table

# COMMAND ----------

table_name = f"{catalog}.default.nyctaxi"
nyctaxidf_prep.write.mode("overwrite").format("delta").saveAsTable(table_name)
display(f"Spark dataframe saved to delta table: {table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  ${sql.catalog}.default.nyctaxi