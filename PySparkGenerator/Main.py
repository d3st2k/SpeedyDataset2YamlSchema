import time
from pyspark.sql import SparkSession
from SchemaGeneration import *
from GeneralMethods import *

"""Basic variables that define current stable version of schemas, "SparkSession" object, paths of where to read and save files and calling functions that generate and dump the schema to those files"""
spark = SparkSession.builder.getOrCreate()

"""Gloabl varibles for major and minor stable metadata and gdpr versions"""
majorMetadataVersion = 1
minorMetadataVersion = 6
metadata = generalMethods(majorMetadataVersion, minorMetadataVersion)

majorGdprVersion = 1
minorGdprVersion = 6
stableGdprVersion = generalMethods(majorGdprVersion, minorGdprVersion)

"""Global input metadata path"""
inputDataFramePath = "Input CSV Metadata/syntheticDataset100K"

"""Global output metadata path"""
outputSchemaPath = "../Output Metadata/Schema Versions"
metadataFileName = "MetadataSpeedTest"
gdprFileName = "GdprSpeedTest"


"""Saving the dataframe into a variable so that we don't have to call the function readCSV() each time!"""
dataFrame = spark.read.csv(f"{inputDataFramePath}.csv", header=True, sep=';').cache()

"""The variable where we save the time each iteration takes"""
time_list = []

"""Variable that defines how many times the program should iterate over the data"""
iteration = 10

"""Iterate 10 times over the same task, so we can get the average time it takes over 10 tests"""
for _ in range(iteration):
    """Start the timer, not including the time it takes to read the csv"""
    beginTime = time.time()

    """Generating the metadata from the dataset and saving it into a variable"""
    primaryArgSchema = datasetTableInfo(dataFrame, "TestMetadata")
    metadataSchema = primaryArgSchema["metadata"]
    gdprSchema = primaryArgSchema["gdpr"]

    """Stop the time after also the spark session has stoped"""
    timeElapsed = time.time() - beginTime

    """Append the time to the overall time consumed for each iteration"""
    time_list.append(timeElapsed)

"""Show how much each iteration has taken"""
print(time_list)

def save_time_speeds(file_path: str, time_list: list, algorithm: str, dataset: str) -> None:
    with open(file_path, 'a') as file:
        file.write("\n")
        file.write(f"Algorithm: {algorithm}\n")
        file.write(f"Dataset: {dataset}\n")
        file.write("Time speeds (in seconds):\n")
        for time_elapsed in time_list:
            file.write(f"{time_elapsed}\n")

algorithm = "Parallel PySpark Speed"
dataset = "10000 rows, 100 tables"
save_time_speeds('../timeSpeeds.txt', time_list, algorithm, dataset)

"""Using generated metadata, output paths and the current latest stable version to dump the metadata into a .yaml schema """
# metadata.writeMetadataFile(metadataSchema, outputSchemaPath, metadataFileName)
# metadata.writeMetadataFile(gdprSchema, outputSchemaPath, gdprFileName)

"""Stop the spark session"""
spark.stop()