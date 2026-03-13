# Multivariate Anomaly Detection with Microsoft Fabric

A comprehensive solution accelerator for performing advanced anomaly detection using data from multiple variables with Microsoft Fabric's real-time intelligence capabilities. 

## 📋 Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Solution Architecture](#solution-architecture)
- [Prerequisites](#prerequisites)
- [Installation Instructions](#installation-instructions)
- [Usage Instructions](#usage-instructions)
- [Solution Components](#solution-components)
- [License](#license)

## 🎯 Overview

This solution accelerator demonstrates how industrial enterprises, energy companies, logistics companies and companies in other industries can leverage Microsoft Fabric to monitor and maintain high-value, densely-instrumented assets. This solution processes telemetry data from industrial equipment, detects subtle anomalies using advanced multivariate anomaly detection algorithms, sends automated alerts and provides comprehensive data visualization capabilities for time series data to facilitate diagnostics and root cause analysis.

The solution simulates a realistic industrial environment with:
- **Sensor telemetry** from two simulated centrifugal compressors, each instrumented with 25 sensors measuring speed, temperature, pressure, axial displacement and vibration.
- **Vehicle tracking** for field service crews and mobile assets
- **Real-time intelligence** using KQL (Kusto Query Language) for immediate operational visibility
- **Anomaly detection** by leveraging multivariate anomaly detection algorithms that enable predictive and conditional maintenance
- **Advanced visualizations** through Power BI reports and real-time dashboards

### Business Value

- **Immediate Operational Visibility**: Monitor equipment statuses in real time
- **Prevent Failures and Outages**: Detect problems early and enable predictive and conditional maintenance
- **Operational Efficiency**: Track field service vehicles and optimize crew dispatch

### Demonstration Video
[![Multivariate Anomaly Detection Demo Video](/media/MVAD%20Demo%20Video%20Screenshot.png)](https://youtu.be/b-LAZZHX20A)


## ✨ Key Features

- **🚀 One-Click Deployment**: Automated installation notebook deploys all Fabric items with dependency management
- **📊 Real-Time Streaming**: Eventstream ingestion for industrial sensor telemetry and vehicle telemetry
- **🔍 Advanced Analytics**: KQL queries and machine learning models for time-series analysis, anomaly detection, and correlation
- **📈 Interactive Dashboards**: Pre-built Power BI reports and KQL dashboards for operational monitoring
- **🎭 Realistic Simulation**: Comprehensive data simulators based on real-world data
- **🏗️ Scalable Architecture**: Built on Microsoft Fabric's lakehouse and eventhouse architecture


## 🏛️ Solution Architecture

### High-Level Architectural Diagram
![High-Level Solution Architecture](/media/Anomaly%20Detection%20Architecture.png)

Note that this diagram represents a hypothetical real-world solution. This solution accelerator replaces source systems with Spark notebooks that generate synthetic data.

### Component Details

#### Data Generation
- **Industrial Telemetry Simulator**: Generate realistic industrial telemetry (using real-world data that is converted into a simulated stream)
- **Vehicle Simulator**: Simulate routes for field service vehicles with speed and heading data

#### Ingestion
- **Eventstreams**: Fabric-native streaming connectors with built-in Event Hub endpoints.

#### Storage
- **Eventhouse (KQL Database)**: Hot path for real-time queries with minimal latency
- **Lakehouse (Delta Tables)**: Storage for reference data

#### Machine Learning
- **Spark Notebooks** are used to train machine learning models using specialized multivariate anomaly detection algorithms.
- **Experiments** are used to track machine learning runs using MLFlow.
- **Machine Learning Models** are registered and used for inferencing.

#### Analytics
- **KQL Queries**: Time-series analysis, aggregations, and correlation queries
- **Power BI Semantic Models**: DirectQuery mode connection to the eventhouse for real-time reporting

#### Visualization
- **Maps**: Real-time visualization of streaming and reference data.
- **Real-Time Dashboards**: Real-time visibility for key operational metrics.
- **Power BI Reports**: Time-series analysis, aggregations, and self-service analytics.

#### Action
- **Activator**: Event data is continuously analyzed and automated notification or actions are initiated when trigger conditions are satisfied.

## 📦 Prerequisites

### Required
- **Microsoft Fabric Capacity**: F16 or higher recommended (Power BI Premium capacity is also supported). Note: this solution includes AI features that are not available on a Fabric Trial capacity. While you will be able to deploy the solution to a workspace on a Trial capacity, some portions of this solution will not work properly.
- **Fabric Workspace**: A workspace with contributor or admin permissions
- **Power BI License**: Power BI Pro or Power BI Premium Per User license.


### Recommended Knowledge
- Basic understanding of Microsoft Fabric concepts (lakehouses, eventhouses, eventstreams)
- Familiarity with KQL (Kusto Query Language) for data exploration (optional)
- Familiarity with machine learning and data science using Spark
- Power BI experience for customizing reports (optional)

## 🚀 Installation Instructions

### Step 1: Create Fabric Workspace
1. Log in to [Microsoft Fabric](https://app.fabric.microsoft.com)
2. Click **Workspaces** → **+ New workspace**
3. Name your workspace (e.g., "Multivariate Anomaly Detection")
4. Assign a Fabric capacity or trial capacity
5. Click **Apply**


### Step 2: Download the Solution Installer notebook
1. Download the [Solution Installer notebook](/deploy/Solution%20Installer.ipynb) to a local folder on your computer.


### Step 3: Import and Run Solution Installer
1. In your Fabric workspace, click **+ New** → **Import notebook**
2. On your local computer, navigate to the folder where you saved the **Solution Installer.ipynb** notebook.
3. Upload and open the **Solution Installer** notebook
4. Click **Run all** to execute the deployment

The installer will:
- ✅ Install required Python packages
- ✅ Download solution files from GitHub
- ✅ Deploy all Fabric items (Eventhouses, Lakehouses, Eventstreams, Notebooks, Reports)
- ✅ Configure item dependencies and relationships
- ✅ Import sample data files
- ✅ Perform post-deployment configuration tasks
- ✅ Configure credentials and refresh all semantic models


### Step 4: Enable Python plugin on the Eventhouse

1. Navigate to the **MultivariateAnomalyDetectionEH** eventhouse in your workspace.
2. Click on **Plugins** and enable the *Python 3.11.7 (preview)* plugin as illustrated below.
![Enable Python Plugin](/media/Enable%20Python%20Extension.png)



## 📖 Usage Instructions

### Running Simulations

#### 1. Start Compressor Event Simulator

```
Location: Simulation/Compressor Event Simulator
Duration: ~10 hours (simulation will end once sample data has been streamed)
Data Generated: batches of industrial telemetry sent out every ~1 second
```

1. Open the **Compressor Event Simulator** notebook
2. Click **Run all**
3. Monitor progress in the output


#### 2. Start Vehicle Tracking Simulation

```
Location: Simulation/Vehicle Telemetry Simulator
Duration: Continuous route playback
Data Generated: GPS coordinates and operational vehicle telemetry every 5 seconds
```

1. Open the **Vehicle Telemetry Simulator** notebook
2. Click **Run all**
3. Vehicles will follow predefined routes with realistic GPS tracking

### Viewing Reports and Dashboards

#### Maps

**Asset Status Map**
- Visualize the health of assets across the enterprise
- Visualize locations of service vehicles

Access: Navigate to **Visualize and Chat** → **Asset Status**

#### Real Time Dashboards

**SCADA Telemetry + Anomalies Dashboard**
- Real-time asset monitoring
- Sensor telemetry monitoring

Access: Navigate to **Visualize and Chat** → **SCADA Telemetry + Anomalies**


#### Power BI Reports

**Time Series - Multivariate Anomaly Analysis**
- Flexible time series analysis of data from multiple sensors
- Single-variable anomaly detection for each time series
- Multivariate anomaly detection
- Diagnostic capabilities for multivariate data
- Descriptive statistics and correlation analysis between selected tags


### Using Activator (for automated alerts and actions)

The **Multivariate Anomaly Activator** enables automatic alerts when trigger conditions are met.

By default, the Activator is configured to generate alerts when multiple moderate severity multivariate anomalies have been detected or when individual high-severity anomalies have been detected. You may configure other triggers using the no-code authoring interface.

Access: Navigate to **Act** → **Multivariate Anomaly Activator**

## 🔧 Troubleshooting
If you encounter challenges with the solution, consider the following steps:
1. Ensure that all pre-requisites have been fully satisfied
1. Ensure that all installation steps have been completed in order
1. Ensure that you have manually enabled the Python plugin on your Eventhouse (as described in the installation instructions above.)
1. Ensure that simulation notebooks are actively running -- it may take a few minutes for simulated data generators to start producing simulated events.


### Getting Help

- **Microsoft Fabric Documentation**: [https://learn.microsoft.com/fabric/](https://learn.microsoft.com/fabric/)
- **Community Forums**: [Fabric Community](https://community.fabric.microsoft.com/)
- **GitHub Issues**: Report bugs or request features in this repository

## 📄 License

This project is provided as-is for demonstration and educational purposes. 

---