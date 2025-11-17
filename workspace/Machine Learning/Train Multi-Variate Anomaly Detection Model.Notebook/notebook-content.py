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
# META     },
# META     "environment": {
# META       "environmentId": "4118ca3e-33ef-a1f0-4874-d332622c2e56",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Train Multi-Variate Anomaly Detection Model for centrifugal compressor 8K2
# 
# 1. Import raw compressor telemetry data from a file
# 1. Clean the data
# 1. Train an anomaly detection model and store in model registry
# 1. Store data as Delta table


# CELL ********************

!pip install time-series-anomaly-detector

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Import required libraries
import numpy as np
import pandas as pd
import mlflow
from anomaly_detector import MultivariateAnomalyDetector

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read raw telemetry data into a Spark dataframe
sdf = spark.sql("SELECT * FROM ReferenceDataLH.compressordata_8K2")
df = sdf.toPandas().sort_values("Timestamp", ascending=True)
df = df.set_index('Timestamp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define features and cutoff dates for training
features_cols = ['8K2_AXL_DISP_01',
       '8K2_AXL_DISP_02',
       '8K2_AXL_DISP_03',
       '8K2_AXL_DISP_04',
       '8K2_PRESS_01',
       '8K2_PRESS_02',
       '8K2_PRESS_03',
       '8K2_PRESS_04',
       '8K2_PRESS_05',
       '8K2_TEMP_01',
       '8K2_TEMP_02',
       '8K2_TEMP_03',
       '8K2_TEMP_04',
       '8K2_TEMP_05',
       '8K2_TEMP_06',
       '8K2_TEMP_07',
       '8K2_TEMP_08',
       '8K2_TEMP_09',
       '8K2_TEMP_10',
       '8K2_TEMP_11',
       '8K2_VIBR_01',
       '8K2_VIBR_02',
       '8K2_VIBR_03',
       '8K2_VIBR_04',
       '8K2_SPD_01',
       ]
training_cutoff_date = pd.to_datetime("2022-09-01")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Split dataframe into a training set and a testing set
train_df = df[df.index < training_cutoff_date]
train_len = len(train_df)
test_len = len(df) - train_len
print(f'Total samples: {len(df)}. Split to {train_len} for training, {test_len} for testing')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define model and set parameters
model = MultivariateAnomalyDetector()
sliding_window = 280
params = {"sliding_window": sliding_window}

#Fit machine learning model
model.fit(train_df, params=params)
with mlflow.start_run():
    mlflow.log_params(params)
    mlflow.set_tag("Training Info", "Multi-variate anomaly detection on compressor telemetry")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Register machine learning model
model_info = mlflow.pyfunc.log_model(
        python_model=model,
        artifact_path="mvad_artifacts",
        registered_model_name="mvad_compressor_model_8K2",
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Return path to model artifacts
mv = mlflow.search_model_versions(filter_string="name='mvad_compressor_model_8K2'")[0]
model_abfss = mv.source
print("Path to model artifacts: " + model_abfss)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
