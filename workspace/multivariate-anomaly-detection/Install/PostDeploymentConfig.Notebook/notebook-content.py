# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# MARKDOWN ********************

# # ⚙️ Post-Deployment Configuration
# 
# - Deploy additional items
# - ImportSampleData
# - Train machine learning models for performing multivariate anomaly detection
# - Configure credentials for semantic models
# - Move items to appropriate folders
#
# Note: This notebook will take about 15 minutes to run. 

# CELL ********************

%pip install fabric-launcher --quiet
import notebookutils
notebookutils.session.restartPython()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#Import required libraries
import sempy.fabric as fabric
from fabric_launcher import create_or_update_fabric_item, get_folder_id_by_name, move_item_to_folder, scan_logical_ids
from fabric_launcher import FabricLauncher

# Download source code from GitHub
launcher = FabricLauncher(notebookutils,
    api_root_url = "https://api.fabric.microsoft.com" #Default is https://api.fabric.microsoft.com, but may vary depending on your environment
    )

extract_to = 'src'
launcher.download_repository(
    repo_owner="slavatrofimov",
    repo_name="Multivariate-Anomaly-Detection-with-Microsoft-Fabric",
    extract_to=extract_to,
    branch="adopt-jumpstart",
)

# Initialize Fabric client and workspace
client = fabric.FabricRestClient()
workspace_id = fabric.get_workspace_id()
repository_directory = f"./{extract_to}/workspace/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

## Define a function to facilitate the deployment of additional items
def deploy_item(item_name, item_type, item_relative_path, folder_name, endpoint, repository_directory, description, client, workspace_id):
    """Use fabric_launcher utilities to deploy an additional item"""

    # Step 1: Scan logical IDs
    print("1. Scanning logical IDs in repository...")
    logical_id_map = scan_logical_ids(
        repository_directory=repository_directory, workspace_id=workspace_id, client=client
    )

    # Step 2: Create/update a custom item with logical ID replacement
    print("2. Creating/updating custom Fabric item...")
    item_id = create_or_update_fabric_item(
        item_name=item_name,
        item_type=item_type,
        item_relative_path=item_relative_path,
        repository_directory=repository_directory,
        workspace_id=workspace_id,
        client=client,
        endpoint=endpoint,
        logical_id_map=logical_id_map,
        description=description,
    )
    print(f"   Item ID: {item_id}")

    # Step 3: Move item to appropriate folder
    print("3. Moving item to target folder...")
    success = move_item_to_folder(
        item_name=item_name,
        item_type=item_type,
        folder_name=folder_name,
        workspace_id=workspace_id,
        client=client
    )

    if success:
        print(f"✅ {item_name} {item_type} created successfully")

# Perform the deployment of the Asset Status Map
try:
    deploy_item(item_name="AssetStatus", item_type="Map", item_relative_path="multivariate-anomaly-detection/VisualizeAndChat/AssetStatus.Map", 
                   folder_name="VisualizeAndChat", endpoint="maps", repository_directory = repository_directory,
                   description="Asset Status Map for Multivariate Anomaly Detection",
                   client = client, workspace_id = workspace_id)
except Exception as e:
    print(f"⚠️ Warning: Failed to deploy AssetStatus Map: {e}")    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Run a notebook to: ImportSampleData
result = launcher.run_notebook_synchronous(
    notebook_name="ImportSampleData",
    parameters={},
    timeout_seconds=3600
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Run a notebook to: TrainMultiVariateAnomalyDetectionModels
result = launcher.run_notebook_synchronous(
    notebook_name="TrainMultiVariateAnomalyDetectionModels",
    parameters={},
    timeout_seconds=3600
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Move machine learning items to the desired folder
move_item_to_folder(
        item_name="MVAD_Compressor_8K2",
        item_type="MLExperiment",
        folder_name="MachineLearning",
        workspace_id=workspace_id,
        client=client
    )

move_item_to_folder(
        item_name="MVAD_Compressor_MBZ",
        item_type="MLExperiment",
        folder_name="MachineLearning",
        workspace_id=workspace_id,
        client=client
    ) 

move_item_to_folder(
        item_name="mvad_compressor_model_8K2",
        item_type="MLModel",
        folder_name="MachineLearning",
        workspace_id=workspace_id,
        client=client
    )

move_item_to_folder(
        item_name="mvad_compressor_model_MBZ",
        item_type="MLModel",
        folder_name="MachineLearning",
        workspace_id=workspace_id,
        client=client
    ) 

print("✅ Experiments and models moved to appropriate folders successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import sempy.fabric as fabric
import requests

### Update data source credentials and refresh semantic models
def update_semantic_model_datasource_credentials(semantic_model_id):
    """
    Update datasource credentials for the selected semantic model using the OAuth access token of the current user and enable SSO
    """    
    try:

        workspace_id = fabric.get_workspace_id()

        client = fabric.PowerBIRestClient()
        datasources_url = f'v1.0/myorg/groups/{workspace_id}/datasets/{semantic_model_id}/datasources'
        datasources_response = client.get(datasources_url)

        if datasources_response.status_code == 200:
            datasources = datasources_response.json().get('value', [])
            print(f"📊 Found {len(datasources)} datasource(s) in semantic model")
            
            # Update credentials for each datasource to use workspace identity
            for datasource in datasources:
                datasource_id = datasource.get('datasourceId')
                gateway_id = datasource.get('gatewayId')
                datasource_type = datasource.get('datasourceType', 'Unknown')
                
                print(f"🔧 Updating credentials for datasource: {datasource_type} (ID: {datasource_id})")
                
                access_token = notebookutils.credentials.getToken('kusto')

                # Prepare credentials payload for workspace identity authentication
                credentials_payload = {
                    "credentialDetails": {
                        "credentialType": "OAuth2",
                        "credentials": "{\"credentialData\":[{\"name\":\"accessToken\", \"value\":\"" + access_token + "\"}]}",
                        "encryptedConnection": "Encrypted",
                        "encryptionAlgorithm": "None",
                        "privacyLevel": "None",
                        "useEndUserOAuth2Credentials": "True",
                        "useCallerAADIdentity": "False"
                    }
                }
                
                # Update datasource credentials
                update_creds_url = f'v1.0/myorg/gateways/{gateway_id}/datasources/{datasource_id}'
                creds_response = client.patch(update_creds_url, json=credentials_payload)
                
                if creds_response.status_code in [200, 201]:
                    print(f"✅ Credentials updated successfully for datasource {datasource_type}")
                else:
                    print(f"⚠️ Warning: Could not update credentials for datasource {datasource_type}: {creds_response.status_code} - {creds_response.text}")
                    
        else:
            print(f"⚠️ Warning: Could not retrieve datasources: {datasources_response.status_code}")

    except Exception as e:
        print(f"⚠️ Warning: Error updating semantic model credentials: {e}")
        raise

def refresh_semantic_model(semantic_model_id):
    """
    Trigger refresh of a semantic model
    """    
    workspace_id = fabric.get_workspace_id()

    try:
        if semantic_model_id:
            # Trigger semantic model refresh
            client = fabric.PowerBIRestClient()
            refresh_url = f'v1.0/myorg/groups/{workspace_id}/datasets/{semantic_model_id}/refreshes'
            refresh_payload = {
                "retryCount": 2
                }
            
            refresh_response = client.post(refresh_url, json=refresh_payload)
            
            if refresh_response.status_code in [200, 201, 202]:
                refresh_data = refresh_response.text
                print(refresh_data)
                print("ℹ️ Triggered semantic model refresh. Refresh will continue in the background.")
            else:
                print(f"⚠️ Warning: Could not trigger semantic model refresh: {refresh_response.status_code} - {refresh_response.text}")
        else:
            print("⚠️ Warning: Cannot trigger refresh - semantic model not found")
            
    except Exception as e:
        print(f"⚠️ Warning: Error triggering semantic model refresh: {e}")
        raise


# Update datasource credentials and refresh all semantic models in the current workspace

# Get all semantic models in the workspace
semantic_models = fabric.list_items(type="SemanticModel")

if semantic_models.empty:
    print("No semantic models found in the workspace.")
else:
    print(f"Total semantic models found: {len(semantic_models)}")
    for index, model in semantic_models.iterrows():
        print("="*60)
        print(f"Starting to process semantic model: {model['Display Name']} (ID: {model['Id']})")
        semantic_model_id = model['Id']
        update_semantic_model_datasource_credentials(semantic_model_id)
        refresh_semantic_model(semantic_model_id)
        print(f"Processed semantic model: {model['Display Name']} (ID: {model['Id']})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Your automated solution deployment is complete!
#
# ⚠️ Please be sure to refresh your browser window to reflect all newly-deployed items!
#
# ## Manual Post-Deployment Tasks
# **Enable Python plugin on the Eventhouse**
# 
# 1. Navigate to the **MultivariateAnomalyDetectionEH** eventhouse in your workspace.
# 2. Click on **Plugins** and enable the *Python 3.11.7 (preview)*
#
# 
# ## Next Steps
#
# Navigate to the "Simulation" folder and run the following two notebooks: "CompressorEventSimulator" and "VehicleTelemetrySimulator".
# 
# Review the README document for details on running simulations and exploring your solution.