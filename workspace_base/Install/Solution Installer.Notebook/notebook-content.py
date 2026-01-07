# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Multivariate Anomaly Detection Installer
# This notebook orchestrates the end-to-end deployment of **Multivariate Anomaly Detection** solution assets into the current Microsoft Fabric workspace using the `fabric-launcher` library.
# 
# This notebook performs the following tasks:
# 1. **🚀 Deployment**: download source code, deploy Fabric items, load reference Data
# 1. **✅ Post-Deployment Tasks**: Complete post-deployment configuration tasks.

# MARKDOWN ********************

# ## 🚀 Deployment
# Download source code, deploy Fabric items, load reference Data

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

from fabric_launcher import FabricLauncher

launcher = FabricLauncher(notebookutils,
    api_root_url = "https://api.fabric.microsoft.com" #Default is https://api.fabric.microsoft.com, but may vary depending on your environment
    )

# Deploy solution with data folders
launcher.download_and_deploy(
    repo_owner="slavatrofimov",
    repo_name="Multivariate-Anomaly-Detection-with-Microsoft-Fabric",
    branch = "accelerator",
    workspace_folder="workspace",
    allow_non_empty_workspace=True,
    item_type_stages = [["KQLDatabase", "Lakehouse", "Eventhouse", "Environment"],["Notebook", "KQLDashboard", "KQLQueryset", "DataPipeline", "Eventstream", "SemanticModel", "Report", "Reflex", "DataAgent"]],
    data_folders={"data": "data"},
    lakehouse_name="ReferenceDataLH",
    validate_after_deployment=True,
    generate_report=True,
    github_token = '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'   
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## ✅ Post-Deployment Tasks

# CELL ********************

#Import required libraries
import sempy.fabric as fabric
from fabric_launcher import create_or_update_fabric_item, get_folder_id_by_name, move_item_to_folder, scan_logical_ids

# Initialize Fabric client and workspace
client = fabric.FabricRestClient()
workspace_id = fabric.get_workspace_id()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

## Deploy the Service Area Map item
def deploy_map(item_name="Asset Status", item_type="Map", item_relative_path="Visualize and Chat/Asset Status.Map", 
               folder_name="Visualize and Chat", endpoint="maps", repository_directory = launcher.repository_path,
               description="Asset Status Map for Multivariate Anomaly Detection",
               client = client, workspace_id = workspace_id):
    """Use fabric_launcher utilities to deploy an additional item: Map"""

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
        print("✅ Map created successfully")

# Perform the deployment of the Service Area Map
deploy_map()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Create additional folders
def create_folder(name):
    try:
        fabric.create_folder(name)
        print(f'Successfully created folder: {name}')
    except:
        print(f'Folder {name} already exists.')

create_folder('Install')
create_folder('Machine Learning')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Move current notebook to the Install folder
move_item_to_folder(
        item_name=notebookutils.runtime.context["currentNotebookName"],
        item_type="Notebook",
        folder_name="Install",
        workspace_id=workspace_id,
        client=client
    )
print("✅ Current notebook moved successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Run a notebook to: Import sample data
result = launcher.run_notebook_synchronous(
    notebook_name="Import Sample Data",
    parameters={},
    timeout_seconds=3600
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Run a notebook to: Train Multi-Variate Anomaly Detection Models
result = launcher.run_notebook_synchronous(
    notebook_name="Train Multi-Variate Anomaly Detection Models",
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
        folder_name="Machine Learning",
        workspace_id=workspace_id,
        client=client
    )

move_item_to_folder(
        item_name="MVAD_Compressor_MBZ",
        item_type="MLExperiment",
        folder_name="Machine Learning",
        workspace_id=workspace_id,
        client=client
    ) 

move_item_to_folder(
        item_name="mvad_compressor_model_8K2",
        item_type="MLModel",
        folder_name="Machine Learning",
        workspace_id=workspace_id,
        client=client
    )

move_item_to_folder(
        item_name="mvad_compressor_model_MBZ",
        item_type="MLModel",
        folder_name="Machine Learning",
        workspace_id=workspace_id,
        client=client
    ) 

print("✅ Experiments and models moved to appropriate folders successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Next Steps
# Your automated solution deploymnet is complete!
# 
# ⚠️ Please be sure to refresh your browser window to reflect all newly-deployed items!
# 
# Review the README document for details on running simulations and exploring your solution.
