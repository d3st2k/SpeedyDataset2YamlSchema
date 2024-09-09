"""Needed packages to run everything in this file"""
from pyspark.sql import DataFrame
from CheckMethods import *

"""Functions that generate all the metadata for different parts of a dataframe """
    
def genPrimaryArgsMeta(title : str, sourceSystem : str, description : str = "A metaschema generated from an example dataset.") -> dict:
    """
    Write general info of metadata file
    ...
    Arguments
    ----------
    title : str
        variable that is gonna be used for the title of the metadata file
    sourceSystem : 
    description : str
        variable that is gonna be used for the description of the metadata file, also has a default value which is gonna be used when there is not value give
    ...
    Returns
    ----------
    It returns a dictionary of the values mentioned above which are gonna be used later to build the entires metadata file
    """
    version = 2
    primaryArgSchema = {
        "version": version,
        "source_system": sourceSystem,
        "title": title,
        "description": f"{description}",
        "tables": []
    }
    return primaryArgSchema
    
def genTableArgsMeta(tableName : str, sourceRef : int, inputSchema : list[dict]) -> dict:
    """
    Generating primary attributes for metadata schema
    ...
    Arguments
    ----------
    tableName : str
        name of the table that we are generating metadata for
    sourceSystem : str
        name of the source system that the table column is part of
    inputSchema : list[dict]
        list of dictionaries that are placed in the `columns` attribute
    ...
    Returns
    ----------
    A dictionary of the primary metadata attributes for the table that we are checking 
    """
    tableArgMeta = {
        "name": f"{tableName}",
        "source": sourceRef,
        "columns": inputSchema,
        "primary_key": "unid",
        "cdc_column": "dml_flag",
        "cdc_type": "soft" 
    }
    return tableArgMeta

def genTableArgsGdpr(tableName : str, inputSchema : list[dict]) -> dict:
    """
    Generating primary attributes for gdpr schema
    ...
    Arguments
    ----------
    tableName : str
        name of the table that we are generating metadata for
    inputSchema : list[dict]
        list of dictionaries that are placed in the `personal < hash` attribute
    ...
    Returns
    ----------
    A dictionary of the primary gdpr attributes for the table that we are checking 
    """
    tableArgGdpr = {
        "name": tableName,
        "personal_data": {"hash" : inputSchema}
    }
    return tableArgGdpr

def genColumnSchema(tableDataframe : DataFrame) -> dict:
    """
    Generating the schema metadata for each column on a table
    ...
    Arguments
    ----------
    tableDataframe : DataFrame
        a piece of the original dataframe that has the column "table_name" equal to a specific table name that enables us to see all the columns in that specific piece of dataframe
    ...
    Returns
    ----------
    Function returns a dictionary that contains metadata for each dataframe column (like: name, data_type, is_nullable, date_format etc)
    """
    columnSchema = {
        "columns": [],
        "hash": []
    }
    for row in tableDataframe:
        columnName = row[5].lower()
        dataType = row[6].lower()
        if "number" in dataType:
            dataType = dataType.replace("number", "decimal")
        if "varchar" in dataType:
            dataType = "string"
        column_info = {
            "name": columnName,
            "data_type": dataType
        }
        if isNull(row[10]):
            column_info["is_nullable"] = True
        if isDate(dataType):
            column_info["date_format"] = "yyyy-MM-dd HH:mm:ss"
        if isGdpr(row[14]):
            columnSchema["hash"].append(columnName)
        columnSchema["columns"].append(column_info)
    return columnSchema