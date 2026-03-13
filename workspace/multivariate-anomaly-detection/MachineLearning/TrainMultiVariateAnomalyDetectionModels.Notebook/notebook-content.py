# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "e98a495b-e623-8e7d-4ec0-3a12d4c7ffdc",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Train Multi-Variate Anomaly Detection Models
# This notebook trains multi-variate anomaly detection models for multiple centrifugal compressor assets (8K2 and MBZ).
# 
# ## Workflow:
# 1. Install required packages
# 1. Configure asset-specific parameters and feature columns
# 1. For each asset:
#    - Retrieve telemetry data from KQL Database
#    - Apply appropriate transformations to the detaframe
#    - Split data into training and testing sets
#    - Train a multi-variate anomaly detection model
#    - Create a separate ML experiment for each asset
#    - Register the model with asset-specific naming
#    - Store model details in the KQL database

# CELL ********************

# Install required packages
%pip install time-series-anomaly-detector==0.3.0 "mlflow<3.0.0"

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
from pyspark.sql import Row
from datetime import datetime

#Import relevant libraries
import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Helper function to retrieve data for the specific asset from a KQL database
def retrieve_kql_asset_telemetry(kusto_query_uri, kql_database, asset_id):
    """
    Retrieve telemetry data from KQL Database for a specific asset.
    
    Parameters:
    - kusto_query_uri: query uri of the Eventhouse
    - kql_database: Name of the KQL database
    - asset_id: Asset identifier to filter data
    
    Returns:
    - Spark DataFrame with columns relevant to the asset
    """
    
    # The access credentials for the write
    kqlAccessToken = mssparkutils.credentials.getToken('kusto')

    kql_query = f"""
    AssetTelemetry('{asset_id}')
    """

    # Execute KQL query using Spark connector
    # Note: Adjust the connection string based on your KQL database setup
    sdf = spark.read.format("com.microsoft.kusto.spark.datasource") \
        .option("kustoCluster", f"{kusto_query_uri}") \
        .option("kustoDatabase", kql_database) \
        .option("kustoQuery", kql_query) \
        .option("accessToken", kqlAccessToken ) \
        .load()

    return sdf

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define configuration for each asset
asset_configs = {
    "8K2": {
        "asset_id": "8K2",
        "training_cutoff_date": "2025-11-13 06:55:36.000",
        "sliding_window": 280
    },
    "MBZ": {
        "asset_id": "MBZ",
        "training_cutoff_date": "2025-11-13 06:55:36.000",
        "sliding_window": 280
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dictionary to store model information for each asset
model_info_dict = {}

# Iterate through each asset configuration
for asset_id, config in asset_configs.items():
    print(f"\n{'='*80}")
    print(f"Training model for asset: {asset_id}")
    print(f"{'='*80}\n")
    
    # Extract configuration parameters
    asset_id = config["asset_id"]
    training_cutoff_date = pd.to_datetime(config["training_cutoff_date"])
    sliding_window = config["sliding_window"]
    
    # Step 1: Retrieve data from KQL Database
    print(f"Step 1: Retrieving data from KQL Database...")
    print(f"  - Asset ID: {asset_id}")
    
    sdf = retrieve_kql_asset_telemetry(kusto_query_uri, kql_database, asset_id)
    print(f"  - Retrieved {sdf.count()} rows")

    # Step 2: Define feature columns and create a Pandas dataframe
    feature_columns = [col for col in sdf.columns if col != 'timestamp']
    df = sdf.toPandas()
    df = df.set_index('timestamp')
    
    # Step 3: Split data into training and testing sets
    print(f"\nStep 3: Splitting data (cutoff: {training_cutoff_date})...")
    
    train_df = df[df.index < training_cutoff_date]
    
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
    experiment_name = f"MVAD_Compressor_{asset_id}"
    mlflow.set_experiment(experiment_name)
    print(f"  - MLflow Experiment: {experiment_name}")
    
    # Fit the model
    model.fit(train_df, params=params)
    
    # Step 5: Log model and parameters to MLflow
    print(f"\nStep 5: Logging model to MLflow...")
    
    with mlflow.start_run(run_name=f"Training_{asset_id}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"):
        # Log parameters
        mlflow.log_params(params)
        mlflow.log_param("asset_id", asset_id)
        mlflow.log_param("training_samples", train_len)
        mlflow.log_param("testing_samples", test_len)
        mlflow.log_param("training_cutoff_date", str(training_cutoff_date))
        mlflow.log_param("num_features", len(feature_columns))
        
        # Log tags
        mlflow.set_tag("Asset", asset_id)
        mlflow.set_tag("Training Info", f"Multi-variate anomaly detection on compressor telemetry for {asset_id}")
        
        # Register the model with asset-specific name
        model_name = f"mvad_compressor_model_{asset_id}"
        print(f"  - Registering model: {model_name}")
        
        model_info = mlflow.pyfunc.log_model(
            python_model=model,
            artifact_path="mvad_artifacts",
            registered_model_name=model_name,
        )
        
        model_info_dict[asset_id] = model_info
        print(f"  - Model registered successfully!")
    
    # Step 6: Retrieve and display model path
    print(f"\nStep 6: Retrieving model artifacts path...")
    mv = mlflow.search_model_versions(filter_string=f"name='{model_name}'")[0]
    model_abfss = mv.source
    print(f"  - Model artifacts path: {model_abfss}")

    # Step 7: Store model details in the KQL Database
    print(f"\nStep 7: Storing model details in KQL database...")

    # The access credentials for the write
    kqlAccessToken = mssparkutils.credentials.getToken('kusto')

    spark.createDataFrame([
        Row(
            asset=asset_id,
            model_uri=model_abfss,
            sliding_window=sliding_window,
            effective_date=datetime.now()
        )
    ]).write.\
    format("com.microsoft.kusto.spark.synapse.datasource").\
    option("kustoCluster",kusto_query_uri).\
    option("kustoDatabase",kql_database).\
    option("kustoTable", "AssetMVADModels").\
    option("accessToken", kqlAccessToken ).\
    option("tableCreateOptions", "CreateIfNotExist").mode("Append").save()
    
    print(f"\n✅ Completed training for asset: {asset_id}\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
