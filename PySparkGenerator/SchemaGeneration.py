"""Needed packages to run everything"""
from pyspark.sql import DataFrame
from concurrent.futures import ThreadPoolExecutor
from SchemaMethods import *

"""All the functions to generate the schemas are called here and that data is saved in the variable "schema\""""

def datasetTableInfo(dataFrame : DataFrame, metadataTitle : str, description : str = "A metaschema generated from an example dataset.") -> dict:
    """
    All the pieces of the whole schema are generated here and are merged together
    ...
    Arguments
    ----------
    dataFrame : DataFrame
        the dataframe that we are gonna generate all the schema for
    metadataTitle : str
        title of the schema file that we are gonna call it
    description : str
        description about the schema, if this argument is not sent the default value is used
    ...
    Returns
    ----------
    The whole generated schema about this dataframe
    """
    firstRow = dataFrame.head(1)[0]
    source = {
        "load_type": "delta",
        "file_pattern": firstRow.FILE_NAME.lower(),
        "params": {
            "decimal_separator": firstRow.DECIMAL_SEPARATOR,
            "format": firstRow.FILE_TYPE.lower()
        }
    }
    sourceSystem = firstRow.SOURCE_SYSTEM
    completeSchema = {
        "metadata": genPrimaryArgsMeta(metadataTitle, sourceSystem, description),
        "gdpr": []
    }
    separatedDataFrames = {}
    for row in dataFrame.collect():
        table_name = row["table_name"]
        row_values = list(row.asDict().values())
        if table_name in separatedDataFrames:
            separatedDataFrames[table_name].append(row_values)
        else:
            separatedDataFrames[table_name] = [row_values]
            
    def processDataframe(key_value):
        key, value = key_value
        sourceRef = source
        columnSchema = genColumnSchema(value)
        tableArgsMeta = genTableArgsMeta(key, sourceRef, columnSchema["columns"])
        tableArgsGdpr = genTableArgsGdpr(key, columnSchema["hash"])
        return tableArgsMeta, tableArgsGdpr

    def parallelProcessDataframes(dataframes):
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = executor.map(processDataframe, dataframes.items())
            for tableArgsMeta, tableArgsGdpr in results:
                completeSchema["metadata"]["tables"].append(tableArgsMeta)
                completeSchema["gdpr"].append(tableArgsGdpr)
        return completeSchema
    
    return parallelProcessDataframes(separatedDataFrames)
        
    