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

# # Train Multi-Variate Anomaly Detection Models for Multiple Assets
# 
# This notebook trains multi-variate anomaly detection models for multiple centrifugal compressor assets (8K2 and MBZ).
# 
# ## Workflow:
# 1. Install required packages
# 1. Configure asset-specific parameters and feature columns
# 1. For each asset:
#    - Retrieve telemetry data from KQL Database
#    - Unpivot the data to wide format for training
#    - Split data into training and testing sets
#    - Train a multi-variate anomaly detection model
#    - Create a separate ML experiment for each asset
#    - Register the model with asset-specific naming
# 
# ## Data Source:
# - Data is retrieved from a KQL Database (similar to scada_telemetry.csv schema)
# - Expected schema: Timestamp, AssetID, TagName, Value
# - Data is unpivoted before training to create feature columns


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
from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuration
# Define parameters for each asset including feature columns, training cutoff dates, and sliding window parameters.


# CELL ********************

# Define configuration for each asset
asset_configs = {
    "8K2": {
        "asset_id": "8K2",
        "kql_table": "ScadaTelemetry",  # KQL table name
        "kql_database": "MultivariateAnomalyDetectionEH",  # KQL database name
        "features": [
            '8K2_AXL_DISP_01', '8K2_AXL_DISP_02', '8K2_AXL_DISP_03', '8K2_AXL_DISP_04',
            '8K2_PRESS_01', '8K2_PRESS_02', '8K2_PRESS_03', '8K2_PRESS_04', '8K2_PRESS_05',
            '8K2_TEMP_01', '8K2_TEMP_02', '8K2_TEMP_03', '8K2_TEMP_04', '8K2_TEMP_05',
            '8K2_TEMP_06', '8K2_TEMP_07', '8K2_TEMP_08', '8K2_TEMP_09', '8K2_TEMP_10', '8K2_TEMP_11',
            '8K2_VIBR_01', '8K2_VIBR_02', '8K2_VIBR_03', '8K2_VIBR_04',
            '8K2_SPD_01'
        ],
        "training_cutoff_date": "2022-09-01",
        "sliding_window": 280,
        "sort_ascending": True  # Sort order for timestamp
    },
    "MBZ": {
        "asset_id": "MBZ",
        "kql_table": "ScadaTelemetry",  # KQL table name
        "kql_database": "MultivariateAnomalyDetectionEH",  # KQL database name
        "features": [
            'MBZ_AXL_DISP_01', 'MBZ_AXL_DISP_02', 'MBZ_AXL_DISP_03', 'MBZ_AXL_DISP_04',
            'MBZ_PRESS_01', 'MBZ_PRESS_02', 'MBZ_PRESS_03', 'MBZ_PRESS_04', 'MBZ_PRESS_05',
            'MBZ_TEMP_01', 'MBZ_TEMP_02', 'MBZ_TEMP_03', 'MBZ_TEMP_04', 'MBZ_TEMP_05',
            'MBZ_TEMP_06', 'MBZ_TEMP_07', 'MBZ_TEMP_08', 'MBZ_TEMP_09', 'MBZ_TEMP_10', 'MBZ_TEMP_11',
            'MBZ_VIBR_01', 'MBZ_VIBR_02', 'MBZ_VIBR_03', 'MBZ_VIBR_04',
            'MBZ_SPD_01'
        ],
        "training_cutoff_date": "2022-04-01",
        "sliding_window": 280,
        "sort_ascending": False  # Sort order for timestamp (note: MBZ uses descending)
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper Functions
# Define functions to retrieve and unpivot data from KQL Database.


# CELL ********************

def retrieve_kql_data(kql_database, kql_table, asset_id):
    """
    Retrieve telemetry data from KQL Database for a specific asset.
    
    Parameters:
    - kql_database: Name of the KQL database
    - kql_table: Name of the KQL table
    - asset_id: Asset identifier to filter data
    
    Returns:
    - Spark DataFrame with columns: Timestamp, AssetID, TagName, Value
    """
    # KQL query to retrieve data for the specific asset
    kql_query = f"""
    {kql_table}
    | where AssetID == '{asset_id}'
    | project Timestamp, AssetID, TagName, Value
    """
    
    # Execute KQL query using Spark connector
    # Note: Adjust the connection string based on your KQL database setup
    sdf = spark.read.format("com.microsoft.kusto.spark.datasource") \
        .option("kustoCluster", f"https://{kql_database}.kusto.fabric.microsoft.com") \
        .option("kustoDatabase", kql_database) \
        .option("kustoQuery", kql_query) \
        .load()
    
    return sdf


def unpivot_telemetry_data(sdf, features):
    """
    Unpivot telemetry data from long format (Timestamp, TagName, Value) to wide format
    with one column per feature.
    
    Parameters:
    - sdf: Spark DataFrame in long format
    - features: List of feature/tag names to unpivot
    
    Returns:
    - Pandas DataFrame with unpivoted data, indexed by Timestamp
    """
    # Convert to Pandas for pivoting
    df = sdf.toPandas()
    
    # Pivot the data: TagName becomes columns, Values fill the cells
    df_wide = df.pivot(index='Timestamp', columns='TagName', values='Value')
    
    # Reset index to make Timestamp a column, then sort and set as index again
    df_wide = df_wide.reset_index()
    
    # Ensure we only keep the features we need
    available_features = [f for f in features if f in df_wide.columns]
    df_wide = df_wide[['Timestamp'] + available_features]
    
    # Convert Timestamp to datetime and set as index
    df_wide['Timestamp'] = pd.to_datetime(df_wide['Timestamp'])
    df_wide = df_wide.set_index('Timestamp')
    
    return df_wide

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Train Models for Each Asset
# Iterate through each asset configuration, retrieve data, train model, and register in MLflow.


# CELL ********************

# Dictionary to store model information for each asset
model_info_dict = {}

# Iterate through each asset configuration
for asset_name, config in asset_configs.items():
    print(f"\n{'='*80}")
    print(f"Training model for asset: {asset_name}")
    print(f"{'='*80}\n")
    
    # Extract configuration parameters
    asset_id = config["asset_id"]
    kql_database = config["kql_database"]
    kql_table = config["kql_table"]
    features_cols = config["features"]
    training_cutoff_date = pd.to_datetime(config["training_cutoff_date"])
    sliding_window = config["sliding_window"]
    sort_ascending = config["sort_ascending"]
    
    # Step 1: Retrieve data from KQL Database
    print(f"Step 1: Retrieving data from KQL Database...")
    print(f"  - Database: {kql_database}")
    print(f"  - Table: {kql_table}")
    print(f"  - Asset ID: {asset_id}")
    
    sdf = retrieve_kql_data(kql_database, kql_table, asset_id)
    print(f"  - Retrieved {sdf.count()} rows")
    
    # Step 2: Unpivot the data
    print(f"\nStep 2: Unpivoting data to wide format...")
    df = unpivot_telemetry_data(sdf, features_cols)
    
    # Sort the dataframe based on configuration
    df = df.sort_index(ascending=sort_ascending)
    print(f"  - Unpivoted data shape: {df.shape}")
    print(f"  - Features: {len(features_cols)} columns")
    
    # Step 3: Split data into training and testing sets
    print(f"\nStep 3: Splitting data (cutoff: {training_cutoff_date})...")
    
    if sort_ascending:
        # For 8K2: training data is before cutoff
        train_df = df[df.index < training_cutoff_date]
    else:
        # For MBZ: training data is after cutoff (due to descending sort)
        train_df = df[df.index > training_cutoff_date]
    
    train_len = len(train_df)
    test_len = len(df) - train_len
    print(f"  - Total samples: {len(df)}")
    print(f"  - Training samples: {train_len}")
    print(f"  - Testing samples: {test_len}")
    
    # Step 4: Train the model
    print(f"\nStep 4: Training multi-variate anomaly detection model...")
    print(f"  - Sliding window: {sliding_window}")
    
    model = MultivariateAnomalyDetector()
    params = {"sliding_window": sliding_window}
    
    # Create a new MLflow experiment for this asset
    experiment_name = f"MVAD_Compressor_{asset_name}"
    mlflow.set_experiment(experiment_name)
    print(f"  - MLflow Experiment: {experiment_name}")
    
    # Fit the model
    model.fit(train_df, params=params)
    
    # Step 5: Log model and parameters to MLflow
    print(f"\nStep 5: Logging model to MLflow...")
    
    with mlflow.start_run(run_name=f"Training_{asset_name}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"):
        # Log parameters
        mlflow.log_params(params)
        mlflow.log_param("asset_id", asset_id)
        mlflow.log_param("training_samples", train_len)
        mlflow.log_param("testing_samples", test_len)
        mlflow.log_param("training_cutoff_date", str(training_cutoff_date))
        mlflow.log_param("num_features", len(features_cols))
        
        # Log tags
        mlflow.set_tag("Asset", asset_name)
        mlflow.set_tag("Training Info", f"Multi-variate anomaly detection on compressor telemetry for {asset_name}")
        
        # Register the model with asset-specific name
        model_name = f"mvad_compressor_model_{asset_name}"
        print(f"  - Registering model: {model_name}")
        
        model_info = mlflow.pyfunc.log_model(
            python_model=model,
            artifact_path="mvad_artifacts",
            registered_model_name=model_name,
        )
        
        model_info_dict[asset_name] = model_info
        print(f"  - Model registered successfully!")
    
    # Step 6: Retrieve and display model path
    print(f"\nStep 6: Retrieving model artifacts path...")
    mv = mlflow.search_model_versions(filter_string=f"name='{model_name}'")[0]
    model_abfss = mv.source
    print(f"  - Model artifacts path: {model_abfss}")
    
    print(f"\n✅ Completed training for asset: {asset_name}\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary
# Display summary of all trained models.


# CELL ********************

print("\n" + "="*80)
print("TRAINING SUMMARY")
print("="*80 + "\n")

print(f"Total assets trained: {len(asset_configs)}\n")

for asset_name in asset_configs.keys():
    model_name = f"mvad_compressor_model_{asset_name}"
    mv = mlflow.search_model_versions(filter_string=f"name='{model_name}'")[0]
    
    print(f"Asset: {asset_name}")
    print(f"  - Model Name: {model_name}")
    print(f"  - Model Version: {mv.version}")
    print(f"  - Run ID: {mv.run_id}")
    print(f"  - Status: {mv.status}")
    print(f"  - Model Path: {mv.source}")
    print()

print("="*80)
print("All models trained and registered successfully! ✅")
print("="*80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
