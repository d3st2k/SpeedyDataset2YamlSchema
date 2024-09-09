
import threading
lock = threading.Lock()

"""Default functions to check if certain values fulfill certain criteria and functions that manipulate with the global "sources" variable"""

"""Mapping string value to boolean so we can use where ever they used"""
strBoolMapping = {
    "y": True,
    "n": False,
    "gdpr": "true"
}

def isNull(nullable : str) -> bool:
    """
    Finding from the dataframe column if that column can have null values (is it required or not)
    ...
    Arguments
    ----------
    nullable : str
        cell value of the row that we are currently checking, on the column "NULLABLE"
    ...
    Returns
    ----------
    Returns a boolean value, based on the variable strBoolMapping
    """
    return strBoolMapping[nullable.lower()]

def isDate(dataType : str) -> bool:
    """
    Function returns extra metadata for column "DATA_TYPE", if that column fulfills a certain condition (is a date type)
    ...
    Arguments
    ----------
    dataType : str
        cell value of the row that we are currently checking, on the column "DATA_TYPE"
    ...
    Returns
    ----------
    Retuns a boolean value, if that specific value has type date
    """
    dateTypeChecked = "date"
    dateBool = dataType.lower() == dateTypeChecked
    return dateBool

def isGdpr(gdprFlag : str) -> bool:
    """
    Function returns extra metadata for column "GDPR_FLAG", if that row fulfills a certain condition (has the cell value "true")
    ...
    Arguments
    ----------
    gdprFlag : str
        cell value of the row that we are currently checking, on the column "GDPR_FLAG"
    ...
    Returns
    ----------
    Boolean value, if the value that we want to compare is equal that to the value that is being compared by 
    """
    gdprBool = gdprFlag.lower() == strBoolMapping["gdpr"]
    return gdprBool
