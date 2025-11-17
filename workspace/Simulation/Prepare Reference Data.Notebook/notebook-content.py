# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7609ffdb-850f-4a84-b77f-0babc2769430",
# META       "default_lakehouse_name": "ReferenceDataLH",
# META       "default_lakehouse_workspace_id": "0cfd1f4d-2f70-495d-86d2-2e5dd9bb0cfd",
# META       "known_lakehouses": [
# META         {
# META           "id": "7609ffdb-850f-4a84-b77f-0babc2769430"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Prepare sample data for 2 centrifugal compressors
# 
# Prepare sample data for two centrifugal compressors. Compressor data is derived from the dataset referenced in the following article: 
# https://ieeexplore.ieee.org/document/11006410
# 
# A. Ntafalias, S. Tsakanikas, P. Papadopoulos, A. Kitsatoglou, S. Chrysovalantis and P. Lamprinoudakis, "Descriptor: Refinery Compressor Sensor Data, One-Year Dataset (RCSD-1YD)," in IEEE Data Descriptions, vol. 2, pp. 173-178, 2025, doi: 10.1109/IEEEDATA.2025.3571011. keywords: {Temperature measurement;Temperature sensors;Monitoring;Vibrations;Reliability;Turbines;Real-time systems;Noise;Accuracy;Data visualization;Compressor monitoring;data acquisition;refineries;sensor data;timeseries},
# 
# The original dataset was downloaded from: https://zenodo.org/records/14866092
# 
# #### Data transformations for Compressor 8K2
# - Rename columns to use more descriptive names
# 
# #### Data transformations for Compressor 8K2
# - Change prefix of each column to MBZ
# - Scale values by 0.873 


# MARKDOWN ********************

# ## Prepare data for Compressor 8K2

# CELL ********************

import pandas as pd
from typing import Union

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read raw telemetry data into a Spark dataframe
sdf = spark.read.format("csv").option("header","true").load("Files/Raw/CentrifugalCompressorTelemetry2022.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Convert Spark dataframe to pandas
df = sdf.toPandas()
# Change column type to float64 for columns: '75ZI800BA.pv', '75ZI800BB.pv' and 23 other columns
df = df.astype({'8K2_AXL_DISP_01': 'float64',
    '8K2_AXL_DISP_02': 'float64',
    '8K2_AXL_DISP_03': 'float64',
    '8K2_AXL_DISP_04': 'float64',
    '8K2_PRESS_01': 'float64',
    '8K2_PRESS_02': 'float64',
    '8K2_PRESS_03': 'float64',
    '8K2_PRESS_04': 'float64',
    '8K2_PRESS_05': 'float64',
    '8K2_TEMP_01': 'float64',
    '8K2_TEMP_02': 'float64',
    '8K2_TEMP_03': 'float64',
    '8K2_TEMP_04': 'float64',
    '8K2_TEMP_05': 'float64',
    '8K2_TEMP_06': 'float64',
    '8K2_TEMP_07': 'float64',
    '8K2_TEMP_08': 'float64',
    '8K2_TEMP_09': 'float64',
    '8K2_TEMP_10': 'float64',
    '8K2_TEMP_11': 'float64',
    '8K2_VIBR_01': 'float64',
    '8K2_VIBR_02': 'float64',
    '8K2_VIBR_03': 'float64',
    '8K2_VIBR_04': 'float64',
    '8K2_SPD_01': 'float64',
    })
df.Timestamp = pd.to_datetime(df.Timestamp)    

#Display the data frame
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert pandas DataFrame to Spark DataFrame, then save as Delta table
spark_df = spark.createDataFrame(df)
spark_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("CompressorData_8K2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Prepare data for Compressor MBZ
# 
# - Change prefix of each column to MBZ
# - Scale values by 0.873 

# CELL ********************

#Load compressor data
sdf = spark.sql("SELECT * FROM ReferenceDataLH.compressordata_8K2")
df = sdf.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def replace_prefix_and_scale(df: pd.DataFrame,
                             old_prefix: str,
                             new_prefix: str,
                             multiplier: float = 0.873,
                             inplace: bool = False) -> pd.DataFrame:
    """
    Replace a leading prefix in column names and multiply all numeric columns by a factor.

    Parameters
    ----------
    df : DataFrame
        Input pandas DataFrame.
    old_prefix : str
        Prefix to look for at the start of each column name.
    new_prefix : str
        Replacement prefix (inserted exactly as provided).
    multiplier : float, default 0.873
        Factor applied to every numeric column.
    inplace : bool, default False
        If True, modifies the DataFrame in place; otherwise returns a new one.

    Returns
    -------
    DataFrame
        Transformed DataFrame (same object if inplace=True).
    """
    target = df if inplace else df.copy()

    # Rename columns with the specified prefix
    rename_map = {
        col: new_prefix + col[len(old_prefix):]
        for col in target.columns
        if col.startswith(old_prefix)
    }
    if rename_map:
        target.rename(columns=rename_map, inplace=True)

    # Scale numeric columns
    num_cols = target.select_dtypes(include="number").columns
    if len(num_cols):
        target.loc[:, num_cols] = target[num_cols] * multiplier

    return target

# Example:
new_df = replace_prefix_and_scale(df, old_prefix="8K2_", new_prefix="MBZ_")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Save resulting data frame into delta table
new_sdf = spark.createDataFrame(new_df)
new_sdf = new_sdf.write.format("delta").mode("overwrite").saveAsTable("compressordata_MBZ")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Prepare tag metadata

# CELL ********************

#Import tag metadata and store as Delta table
stmdf = spark.read.format("csv").option("header","true").load("Files/Raw/TagMetadata.csv")
tmdf = stmdf.toPandas()
tmdf = tmdf.astype({'threshold_low': 'float64','threshold_high': 'float64'})
spark_tmdf = spark.createDataFrame(tmdf)
spark_tmdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("TagMetadata")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --Insert metadata for the second compressor
# MAGIC INSERT INTO tagmetadata
# MAGIC SELECT replace(tag_id, '8K2', 'MBZ') as tag_id,
# MAGIC replace(tag_description, '8K2', 'MBZ') as tag_description,
# MAGIC category,
# MAGIC threshold_low,
# MAGIC threshold_high,
# MAGIC threshold_type,
# MAGIC agrregation_rule,
# MAGIC uom,
# MAGIC legacy_tag_id
# MAGIC FROM tagmetadata

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Prepare Asset Data

# CELL ********************

#Import asset metadata and store as Delta table
stmdf = spark.read.format("csv").option("header","true").load("Files/Raw/assets.csv")
tmdf = stmdf.toPandas()
tmdf = tmdf.astype({'Latitude': 'float64','Longitude': 'float64'})
spark_tmdf = spark.createDataFrame(tmdf)
spark_tmdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("assets")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
