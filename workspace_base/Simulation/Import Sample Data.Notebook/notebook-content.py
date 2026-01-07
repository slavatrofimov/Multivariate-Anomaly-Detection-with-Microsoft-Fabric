# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ccffad8e-d4fc-4b7d-b3a0-74052b432e9a",
# META       "default_lakehouse_name": "ReferenceDataLH",
# META       "default_lakehouse_workspace_id": "ee5caca6-254f-4f4f-9642-236ba78303d4",
# META       "known_lakehouses": [
# META         {
# META           "id": "ccffad8e-d4fc-4b7d-b3a0-74052b432e9a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Import sample data for two centrifugal compressors
# 
# Prepare sample data for two centrifugal compressors. Compressor data is derived from the dataset referenced in the following article: 
# A. Ntafalias, S. Tsakanikas, P. Papadopoulos, A. Kitsatoglou, S. Chrysovalantis and P. Lamprinoudakis, "Descriptor: Refinery Compressor Sensor Data, One-Year Dataset (RCSD-1YD)," in IEEE Data Descriptions, vol. 2, pp. 173-178, 2025, doi: 10.1109/IEEEDATA.2025.3571011.
# https://ieeexplore.ieee.org/document/11006410
# 
# The original dataset was downloaded from: https://zenodo.org/records/14866092
# 
# **Data transformations applied to the original dataset:**
# - Renamed columns to use more descriptive names
# - Unpivoted data to transform from wide to long format
# - Scaled data to a different time period with higher event frequency (from 15 minutes to 1-2 seconds)
# - Simulated multiple compressors (Compressor 8K2 and Compressor MBZ)
# - Reordered records and scaled values for Compressor MBZ


# MARKDOWN ********************

# ## Import data and save as Delta tables

# CELL ********************

# MAGIC %%html
# MAGIC # Read raw telemetry data into a Spark dataframe and save as Delta Table
# MAGIC spark.read.format("parquet") \
# MAGIC     .option("header", "true") \
# MAGIC     .load("Files/data/scada_telemetry.parquet") \
# MAGIC     .write.format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .option("overwriteSchema", "true") \
# MAGIC     .saveAsTable("telemetry") 
# MAGIC 
# MAGIC #Import tag metadata and store as Delta table
# MAGIC spark.read.format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .option("inferSchema", "true") \
# MAGIC     .load("Files/data/tags.csv") \
# MAGIC     .write.format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .option("overwriteSchema", "true") \
# MAGIC     .saveAsTable("tags")
# MAGIC 
# MAGIC #Import asset metadata and store as Delta table
# MAGIC spark.read.format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .option("inferSchema", "true") \
# MAGIC     .load("Files/data/assets.csv") \
# MAGIC     .write.format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .option("overwriteSchema", "true") \
# MAGIC     .saveAsTable("assets")
# MAGIC 
# MAGIC #Import asset features for multivariate anomaly detection and store as Delta table
# MAGIC spark.read.format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .option("inferSchema", "true") \
# MAGIC     .load("Files/data/asset_mvad_features.csv") \
# MAGIC     .write.format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .option("overwriteSchema", "true") \
# MAGIC     .saveAsTable("asset_mvad_features")

# METADATA ********************

# META {
# META   "language": "html",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load sample data to the Eventhouse

# CELL ********************

#Import relevant libraries
import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Import relevant libraries
import sempy.fabric as fabric

# Get Kusto Query URI for a given eventhouse
def get_kusto_query_uri(eventhouse_name):
    workspace_id = fabric.resolve_workspace_id()
    eventhouse_id = fabric.resolve_item_id(eventhouse_name)
    client = fabric.FabricRestClient()
    url = f"v1/workspaces/{workspace_id}/eventhouses/{eventhouse_id}"
    response = client.get(url)
    kusto_query_uri = response.json()['properties']['queryServiceUri']
    return kusto_query_uri

kusto_query_uri = get_kusto_query_uri('MultivariateAnomalyDetectionEH')

# The database to write the data
kql_database = "MultivariateAnomalyDetectionEH"

# The access credentials for the write
kqlAccessToken = mssparkutils.credentials.getToken('kusto')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Import tag metadata and store in the KQL database
spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/data/tags.csv") \
    .write.\
    format("com.microsoft.kusto.spark.synapse.datasource").\
    option("kustoCluster",kusto_query_uri).\
    option("kustoDatabase",kql_database).\
    option("kustoTable", "Tags").\
    option("accessToken", kqlAccessToken ).\
    option("tableCreateOptions", "CreateIfNotExist").mode("Append").save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Import asset metadata and store in the KQL database
spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/data/assets.csv") \
    .write.\
    format("com.microsoft.kusto.spark.synapse.datasource").\
    option("kustoCluster",kusto_query_uri).\
    option("kustoDatabase",kql_database).\
    option("kustoTable", "Assets").\
    option("accessToken", kqlAccessToken ).\
    option("tableCreateOptions", "CreateIfNotExist").mode("Append").save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Import asset features for multivariate anomaly detection and store in the KQL database
spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/data/asset_mvad_features.csv") \
    .write.\
    format("com.microsoft.kusto.spark.synapse.datasource").\
    option("kustoCluster",kusto_query_uri).\
    option("kustoDatabase",kql_database).\
    option("kustoTable", "AssetMVADFeatures").\
    option("accessToken", kqlAccessToken ).\
    option("tableCreateOptions", "CreateIfNotExist").mode("Append").save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read raw telemetry data into a Spark dataframe and save into the KQL database
# This will seed the ScadaTelemetry table with historical data
# Historical data will be used for initial model training and initial anomaly detection
spark.read.format("parquet") \
    .option("header", "true") \
    .load("Files/data/scada_telemetry.parquet") \
    .write.\
    format("com.microsoft.kusto.spark.synapse.datasource").\
    option("kustoCluster",kusto_query_uri).\
    option("kustoDatabase",kql_database).\
    option("kustoTable", "ScadaTelemetry").\
    option("accessToken", kqlAccessToken ).\
    option("tableCreateOptions", "CreateIfNotExist").mode("Append").save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
