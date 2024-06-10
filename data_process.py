# Databricks notebook source
try:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()
except ImportError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %pip install langchain langchain-community langchain-core pyspark_ai mlflow

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df=spark.table('silver_transportation.nypd_motor_vehicle_collisions')

# COMMAND ----------

from langchain_community.chat_models import ChatDatabricks
from pyspark_ai import SparkAI

# COMMAND ----------

llm=ChatDatabricks(endpoint="databricks-dbrx-instruct")

# COMMAND ----------


spark_ai = SparkAI(spark_session=spark, llm=llm)
spark_ai.activate()

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession object
#spark = SparkSession.builder.getOrCreate()

df.ai.plot("what has the highest contribution to motor injuries")

# COMMAND ----------

df.ai.plot("scatter chart for transportation, only top 10 zipcode has the most killed incident? take zipcode as string")

# COMMAND ----------

df.ai.plot("pie chart for transportation, show top 5 zip code has the most fatality incident?")

# COMMAND ----------

dbutils.widgets.text("question",defaultValue="")

# COMMAND ----------

question=dbutils.widgets.get("question")

# COMMAND ----------

question

# COMMAND ----------

df.ai.plot(question)

# COMMAND ----------

from langchain_core.runnables import chain

# COMMAND ----------

@chain
def transport_chatbot_model(question):
    return df.ai.plot(question)

# COMMAND ----------

transport_chatbot_model.invoke(question)

# COMMAND ----------

def transport_chatbot(question):
    return df.ai.plot(question)

# COMMAND ----------

import mlflow

# COMMAND ----------

from mlflow.models import infer_signature
import mlflow
import langchain

mlflow.set_registry_uri("hackthon-transportation")
model_name = f"workspace.silver_transportation.trans_chatbot_model"

with mlflow.start_run(run_name="trans_chatbot_rag") as run:
    signature = infer_signature(question)
    model_info = mlflow.langchain.log_model(
        transport_chatbot_model,
        artifact_path="trans_chatbot",
        registered_model_name=model_name,
        pip_requirements=[
            "mlflow==" + mlflow.__version__,
            "langchain==" + langchain.__version__,
            "langchain-community", 
            "langchain-core", 
            "pyspark_ai", 
            "mlflow",
        ],
        input_example=question,
        signature=signature
    )

# COMMAND ----------


