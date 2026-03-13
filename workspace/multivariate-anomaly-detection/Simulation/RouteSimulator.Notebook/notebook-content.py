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

# # Vehicle Route Generator
# 
# Generates realistic vehicle routes using Azure Maps APIs to find points of interest and calculate road network paths.
# 
# **Features**: POI discovery via Azure Maps, realistic route calculation following road networks, route segmentation for smooth telemetry playback  
# **Output**: JSON file with route coordinates saved to Lakehouse Files  
# **Duration**: Quick (~1-2 minutes depending on API calls and route count)  
# **Requirements**: Azure Maps subscription key stored in Azure Key Vault
# 
# <mark>Note: this notebook requires a subscription to the Azure Maps and relies on Azure Key Vault to securely store and retrieve API Keys. However, this notebook is optional -- sample vehicle route data has been pre-loaded into the files area of the Lakehouse and will be used even if you do not execute this notebook.</mark>


# CELL ********************

%pip install shapely --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
import shapely
import random
import json
import math
import sys

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Retrieve Azure Maps subscription key from Key Vault

AzureKeyVaultName = 'https://<yourname>.vault.azure.net/'
AzureKeyVaultSecretName = 'MySecretName'

try:
    AzureMapsKey = notebookutils.credentials.getSecret(AzureKeyVaultName, AzureKeyVaultSecretName)
    print("✅ Azure Maps key retrieved from Key Vault")
except Exception as e:
    print(f"❌ Error retrieving Azure Maps key.")
    print("   In order to continue, create an Azure Maps account, store API Key in the Key Vault and update AzureKeyVaultName and AzureKeyVaultSecretName variables.")
    print("✅  Keep in mind, this notebook is optional -- sample vehicle route data has been pre-loaded into the files area of the Lakehouse and will be used even if you do not execute this notebook.")
    notebookutils.session.stop()

# Define POI search parameters
query = 'Restaurant'  # Type of points of interest to visit
radius = 50000  # Search radius in meters

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_POIs(latitude, longitude, query, radius, subscription_key):
    """Get points of interest from Azure Maps with error handling"""
    
    # Azure Maps Search POI Category API URL
    url = 'https://atlas.microsoft.com/search/poi/category/json'

    # Parameters for the API call
    params = {
        'api-version': 1.0,
        'subscription-key': subscription_key,
        'query': query,
        'lat': latitude,
        'lon': longitude,
        'radius': radius,
        'limit': 100
    }

    try:
        # Make the API call with timeout
        response = requests.get(url, params=params, timeout=30)

        # Check if the request was successful
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Azure Maps POI API error: {response.status_code} - {response.text[:200]}")
            return None
            
    except requests.Timeout:
        print("❌ Azure Maps API request timed out")
        return None
    except Exception as e:
        print(f"❌ Error calling Azure Maps POI API: {str(e)[:150]}")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_waypoints(start, POIs, end):
    # Start with starting point
    waypoints = str(start[0]) + "," + str(start[1]) + ":"
    
    for result in POIs['results']:
        waypoint = str(result['position']['lat']) + "," + str(result['position']['lon']) + ":"
        waypoints += waypoint
    
    # End with ending point
    waypoints += str(start[0]) + "," + str(start[1]) + ":"
    # Remove the trailing colon
    waypoints = waypoints.rstrip(":")
    
    return waypoints

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_route(waypoints, subscription_key):

    # Azure Maps Route Directions API URL
    url = 'https://atlas.microsoft.com/route/directions/json'

    # Parameters for the API call
    params = {
        'api-version': 1.0,
        'subscription-key': subscription_key,
        'query': waypoints,
        'report': 'effectiveSettings',
        'travelMode': 'truck',
        'computeBestOrder': 'true',
        'routeType':'shortest'
    }

    # Headers
    headers = {
        'Content-Type': 'application/json',
        'subscription-key': subscription_key
    }

    # Make the API call
    response = requests.get(url, headers=headers, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the response JSON
        return response.json()
    else:
        print(f"Failed to retrieve route directions: {response.status_code} - {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

def get_route_segments(route):
    points = []
    
    for route in route["routes"]:
        for leg in route["legs"]:
            for point in leg["points"]:
                #segment = "(" + str(point['latitude']) + "," + str(point['longitude']) + ")"
                coords = [point['latitude'], point['longitude']]
                points.append(coords)
    
    route_line_string = shapely.LineString(points)
    route_segments = shapely.segmentize(route_line_string, max_segment_length=.0002)
    route_segment_coordinates = shapely.get_coordinates(route_segments)
    return route_segment_coordinates.tolist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_route_points(coordinates, AzureMapsKey):
    latitude = float(coordinates[0])
    longitude = float(coordinates[1])

    POIs = get_POIs(latitude, longitude, query, radius, AzureMapsKey)
    waypoints = get_waypoints([latitude, longitude], POIs, [latitude, longitude])
    route = get_route(waypoints, AzureMapsKey)
    route_points = get_route_segments(route)
    return route_points

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Initialize the routes that will serve as the starting points for route planning
routes = [
    {"route_id": "75201", "centroid_coordinates": (32.7876, -96.7990)},  # Dallas (North Texas)
    {"route_id": "76102", "centroid_coordinates": (32.7555, -97.3308)},  # Fort Worth (North Texas)
    {"route_id": "77002", "centroid_coordinates": (29.7550, -95.3650)},  # Houston (East Texas)
    {"route_id": "78205", "centroid_coordinates": (29.4246, -98.4911)},  # San Antonio (South Texas)
    {"route_id": "78701", "centroid_coordinates": (30.2711, -97.7437)},  # Austin (Central Texas)
    {"route_id": "79901", "centroid_coordinates": (31.7587, -106.4869)}, # El Paso (Far West Texas)
    {"route_id": "79701", "centroid_coordinates": (31.9974, -102.0779)}, # Midland (West Texas)
    {"route_id": "79401", "centroid_coordinates": (33.5866, -101.8850)}, # Lubbock (Northwest Texas)
    {"route_id": "76301", "centroid_coordinates": (33.9137, -98.4934)},  # Wichita Falls (North Texas)
    {"route_id": "78501", "centroid_coordinates": (26.2034, -98.2300)},  # McAllen (South Texas)
    {"route_id": "78332", "centroid_coordinates": (27.7500, -98.0700)},  # Alice (South Texas)
    {"route_id": "77630", "centroid_coordinates": (30.0935, -93.7366)},  # Orange (East Texas)
    {"route_id": "75901", "centroid_coordinates": (31.3382, -94.7291)},  # Lufkin (East Texas)
    {"route_id": "76401", "centroid_coordinates": (32.2200, -98.2170)},  # Stephenville (Central Texas)
    {"route_id": "79015", "centroid_coordinates": (34.7234, -101.3929)}  # Canyon (Panhandle)
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get route points
for route in routes:
    route["points"] = get_route_points(route['centroid_coordinates'], AzureMapsKey)
    route["total_points"] = len(route["points"])
    route['current_point'] = random.randint(0, route["total_points"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os

#Save routes to a Lakehouse file

# Target path in Lakehouse Files
output_path = "/lakehouse/default/Files/data/vehicle_route_points.json"

# Ensure directory exists
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Write JSON to file (pretty-printed)
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(routes, f, indent=2, ensure_ascii=False)

print(f"✅ JSON written to: {output_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display the routes and count the number of points
for route in routes:
    display('Route ' + route['route_id'] + ' has ' + str(len(route['points'])) + ' points and is shaped as follows:')
    display(shapely.LineString(route["points"]))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
