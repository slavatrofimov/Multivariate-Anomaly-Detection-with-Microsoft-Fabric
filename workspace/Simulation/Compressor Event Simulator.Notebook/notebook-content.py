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

# # Compressor Data Stream Simulator
# ### Simulate streaming data for Realtime Analytics 
# This notebook will read a delta table and will send it to an event hub endpoint as a stream of events. The notebook will send a certain number of lines for each batches according to the "BatchSize" Parameter. The number of batch size is computed automatically according to the total number of lines and batch size and the while loop will stop once the file has been streamed completely.  

# MARKDOWN ********************

# ### 1. Set the parameters

# CELL ********************

# Configure Azure Event Hub connection parameters
scada_event_hub_connection_string = spark.sql("SELECT value FROM ReferenceDataLH.secrets WHERE name = 'scada_event_hub_connection_string'").first()[0]
anomalies_event_hub_connection_string = spark.sql("SELECT value FROM ReferenceDataLH.secrets WHERE name = 'anomalies_event_hub_connection_string'").first()[0]

kustoUri = "https://trd-xarh0n5kkf5xbfcwyc.z4.kusto.fabric.microsoft.com"

# Set batch size (i.e. number of rows from the CSV that being sent at once. use a higher number when wanting a more rapid movement on the report)
BatchSize = 1
anomalyBatchSize = 10

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Install required libraries and import dependencies


# CELL ********************

pip install azure-eventhub>=5.11.0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
import os
import datetime
import json
import math
from azure.eventhub import EventHubProducerClient, EventData

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Helper functions for async execution

# CELL ********************

# Async utilities for a PySpark notebook
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from functools import wraps
from typing import Any, Callable, Iterable, List, Optional, Union
import traceback

# Reuse a global thread pool (tune max_workers as needed)
_ASYNC_EXECUTOR = ThreadPoolExecutor(max_workers=4)

def submit_async(fn: Callable, *args, **kwargs) -> Future:
    """
    Submit a callable to run asynchronously in a background thread.
    Returns a concurrent.futures.Future.
    """
    return _ASYNC_EXECUTOR.submit(fn, *args, **kwargs)

def async_task(fn: Callable) -> Callable:
    """
    Decorator: calling the function schedules it asynchronously and returns a Future.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs) -> Future:
        return submit_async(fn, *args, **kwargs)
    return wrapper

def gather(futures: Iterable[Future], timeout: Optional[float] = None) -> List[Any]:
    """
    Wait for all futures, raising the first exception encountered.
    Returns list of results in the same order as provided.
    """
    futures = list(futures)
    results = [None] * len(futures)
    index_map = {f: i for i, f in enumerate(futures)}
    for f in as_completed(futures, timeout=timeout):
        idx = index_map[f]
        results[idx] = f.result()  # will raise if the task failed
    return results

def safe_submit(fn: Callable, *args, **kwargs) -> Future:
    """
    Wrap submission so exceptions are captured with stack traces logged.
    """
    def _wrapped():
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            traceback.print_exc()
            raise e
    return submit_async(_wrapped)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define a function to transform Pandas dataframe row into a set of messages for telemetry
def rows_to_metrics_json(df, timestamp_col="Timestamp"):
    """
    Accepts a list of Spark DataFrame rows (Row objects) and returns a JSON string
    with each cell (except timestamp) as a separate object in the 'metrics' array.
    """
    ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
    metrics = []
    for row_idx, row in df.iterrows():
        for col in df.columns:
            if col == timestamp_col:
                    continue
            cell_value = row[col]
            metric = {
                "name": col,
                "timestamp": ts,
                "dataType": "Double",  
                "value": cell_value
                }
            metrics.append(metric)
    return json.dumps({"metrics": metrics}, default=str)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Define a function to transform Pandas dataframe row into a set of messages for anomalies
def rows_to_anomalies_json(df):
    """
    Accepts a list of Spark DataFrame rows (Row objects) and returns a JSON string
    """
    anomalies = []
    for row_idx, row in df.iterrows():
        anomaly = {
            "is_anomaly": row["is_anomaly"],
            "asset": row["asset"],
            "timestamp": row["timestamp"],
            "score": row["score"],
            "severity": row["severity"],
            "interpretation": row["interpretation"]
            }
        anomalies.append(anomaly)
    return json.dumps({"anomalies": anomalies}, default=str)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to detect and store anomalies in a KQL database
def detect_and_store_anomalies_kql(asset):
    # Get anomalies using multivariate anomaly detection
    kustoQuery = 'mvad_get_new_anomalies("' + asset + '")'

    # The database with data to be read.
    database = "MultivariateAnomalyDetectionEH"
    # The access credentials.
    accessToken = mssparkutils.credentials.getToken('kusto')

    try:
        kustoDf  = spark.read\
        .format("com.microsoft.kusto.spark.synapse.datasource")\
        .option("accessToken", accessToken)\
        .option("kustoCluster", kustoUri)\
        .option("kustoDatabase", database)\
        .option("kustoQuery", kustoQuery).load()
    except Exception as e:
        print(e) 

    kustoDf = kustoDf.toPandas()    

    # Determine the row count of the file
    z = len(kustoDf)

    #Set some control variables
    i            = anomalyBatchSize 
    x            = 0     # We open the batch at the first row by array index so we stat at 0
    y            = x+i   # We seal the batch at Start + Increment(i)
    BatchCounter = 0     # Initializing a batch counter
    RowCounter   = 0     # Initializine a Row counter
    TargetBatchCount = z/i if z%i ==0 else math.ceil(z/i) # Adding an additional batch if (RowCount / BatchSize) has a residual to catch them.

    while BatchCounter < TargetBatchCount:

        BatchCounter = BatchCounter + 1 # == Move our batch counter one notch up  
        #Instantiate an event hub producer
        anomalyProducer = EventHubProducerClient.from_connection_string(conn_str=anomalies_event_hub_connection_string)

        anomalyBatch = anomalyProducer.create_batch()     # == Instantiate the batch
        json_result = rows_to_anomalies_json(kustoDf[x:y])
        anomalyBatch.add(EventData(json_result))
        anomalyProducer.send_batch(anomalyBatch)          # == Send the batch to Event hub!
        anomalyProducer.close()                # == Clean up the batch

        #Setting the control variable for the next pass
        RowCounter   = RowCounter + i
        RowRemaining = max(0,(z-RowCounter))
        x = y
        y = x+i if RowRemaining > i else x+RowRemaining

        print('Number of anomalies for asset ' + asset + ': ' + str(kustoDf.count()))

    return kustoDf.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. Send events to EventHub

# CELL ********************

#Read in the comressor data into a dataframe and transform to Pandas
df1 = spark.sql("SELECT * FROM ReferenceDataLH.compressordata_8K2").toPandas().sort_values("Timestamp", ascending=True)
df1 = df1.reset_index(drop=True)
#For the second compressor, we intentionally sort the data in reverse order to simulate differences
df2 = spark.sql("SELECT * FROM ReferenceDataLH.compressordata_MBZ").toPandas().sort_values("Timestamp", ascending=False)
df2 = df2.reset_index(drop=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Instantiate an event hub producer
producer = EventHubProducerClient.from_connection_string(conn_str=scada_event_hub_connection_string)

# Determine the row count of the two data frames
z1 = len(df1)
z2 = len(df2) 
z = min(z1,z2)


#Set some control variables
i            = BatchSize 
x            = 0     # We open the batch at the first row by array index so we stat at 0
y            = x+i   # We seal the batch at Start + Increment(i)
BatchCounter = 0     # Initializing a batch counter
RowCounter   = 0     # Initializine a Row counter
TargetBatchCount = z/i if z%i ==0 else math.ceil(z/i) # Adding an additional batch if (RowCount / BatchSize) has a residual to catch them.

print ('====================================')
print ('Target batch count should be: '+ str(TargetBatchCount))
print ('====================================')
print ('Beginning stream...')
print ('====================================')

while BatchCounter < TargetBatchCount:

    BatchCounter = BatchCounter + 1 # == Move our batch counter one notch up  
    b = producer.create_batch()     # == Instantiate the batch
    json_result1 = rows_to_metrics_json(df1[x:y])
    json_result2 = rows_to_metrics_json(df2[x:y])
    b.add(EventData(json_result1))
    b.add(EventData(json_result2))
    producer.send_batch(b)          # == Send the batch to Event hub!
    time.sleep(1)                   # == We add an intentional 1s pause
    producer.close()                # == Clean up the batch
    #Kick-of detection of multivariate anomalies after each set of 180 batches
    if BatchCounter >= 300 and BatchCounter % 240 == 120:
        # Submit asynchronously
        future = submit_async(detect_and_store_anomalies_kql('8K2'))
    if BatchCounter >= 300 and BatchCounter % 240 == 0:
        # Submit asynchronously
        future = submit_async(detect_and_store_anomalies_kql('MBZ'))
    # Printing some stats to track the stream
    if BatchCounter %60==0:                
        print ('--Batch #:' + str(BatchCounter) + '; rows: ' + str(x) + ' - ' + str(y)) 
    #Setting the control variable for the next pass
    RowCounter   = RowCounter + i
    RowRemaining = max(0,(z-RowCounter))
    x = y
    y = x+i if RowRemaining > i else x+RowRemaining
    

print ('====================================')  
print ('End of stream reached')
print ('====================================')    
print ('Number of batches was: '  + str(BatchCounter))
print ('Last batch was from row: '+ str(x) + ' to row: '+ str(y)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
