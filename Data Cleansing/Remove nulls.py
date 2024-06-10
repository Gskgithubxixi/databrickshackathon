# Databricks notebook source
from pyspark.sql import functions as F

df = table('john_snow_labs_transportation.transportation.nypd_motor_vehicle_collisions')

cleansedDf = (
    df.withColumn('ID', F.monotonically_increasing_id()+1)
    .withColumn('Number_Of_Persons_Injured', F.nvl(F.col('Number_Of_Persons_Injured'), F.lit('0')))
    .withColumn('Number_Of_Persons_Killed', F.nvl(F.col('Number_Of_Persons_Killed'), F.lit('0')))
    .withColumn('Number_Of_Pedestrians_Injured', F.nvl(F.col('Number_Of_Pedestrians_Injured'), F.lit('0')))
    .withColumn('Number_Of_Pedestrians_Killed', F.nvl(F.col('Number_Of_Pedestrians_Killed'), F.lit('0')))
    .withColumn('Number_Of_Motorist_Injured', F.nvl(F.col('Number_Of_Motorist_Injured'), F.lit('0')))
    .withColumn('Number_Of_Cyclist_Injured', F.nvl(F.col('Number_Of_Cyclist_Injured'), F.lit('0')))
    .withColumn('Number_Of_Cyclist_Killed', F.nvl(F.col('Number_Of_Cyclist_Killed'), F.lit('0')))
    .withColumn('Number_Of_Motorist_Injured', F.nvl(F.col('Number_Of_Motorist_Injured'), F.lit('0')))
    .withColumn('Number_Of_Motorist_Killed', F.nvl(F.col('Number_Of_Motorist_Killed'), F.lit('0')))
    .withColumn('Contributing_Factors_Vehicle_1', F.nvl(F.col('Contributing_Factors_Vehicle_1'), F.lit('Unspecified')))
    .withColumn('Contributing_Factors_Vehicle_2', F.nvl(F.col('Contributing_Factors_Vehicle_2'), F.lit('Unspecified')))
    .withColumn('Contributing_Factors_Vehicle_3', F.nvl(F.col('Contributing_Factors_Vehicle_3'), F.lit('Unspecified')))
    .withColumn('Contributing_Factors_Vehicle_4', F.nvl(F.col('Contributing_Factors_Vehicle_4'), F.lit('Unspecified')))
    .withColumn('Contributing_Factors_Vehicle_5', F.nvl(F.col('Contributing_Factors_Vehicle_5'), F.lit('Unspecified')))
)

display(cleansedDf)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from john_snow_labs_transportation.transportation.nypd_motor_vehicle_collisions where Number_Of_Persons_Injured is null

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA silver_transportation

# COMMAND ----------

spark.sql("DROP TABLE silver_transportation.nypd_motor_vehicle_collisions")

# COMMAND ----------

# DBTITLE 1,Save table
cleansedDf.write.mode("overwrite").saveAsTable("silver_transportation.nypd_motor_vehicle_collisions")
