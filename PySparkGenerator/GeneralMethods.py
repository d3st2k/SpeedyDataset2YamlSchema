"""Needed packages to run everything"""
import yaml

"""General methods that are dictate what stable version the file is currently in, how the file is read, created and how the metadata is dumped into it"""

class generalMethods:
    def __init__(self, majorVersion: int, minorVersion : int) -> None:
        """
        Constructor of the class general methods 
        ...
        Arguments
        ----------
        self: 
            variable that calls this class to create an instance
        majorVersion : int
            current major version that is stable for instance
        minorVersion : int
            current minor version that is stable for instance
        ...
        Returns
        ----------
        None
        """
        self.majorVersion = majorVersion
        self.minorVersion = minorVersion
        self.stableVersion = f"{self.majorVersion}.{self.minorVersion}"

    def writeMetadataFile(self, schema: dict, outputMetadataPath: str, fileName: str, outputFileType : str = "yaml") -> None:
        """
        Write generated metadata, with the current stable version
        ...
        Arguments
        ----------
        schema : dict
            dictionary that contains all the data that needs to be inputed inside the .yaml file to structure the metadata
        outputMetadataPath : str
            path of where the metadata files are going to be saved, so that they are structured and easy to find
        fileName : str
            path of where each folder is gonna be grouped based on the type of the file
        stableVersion : str
            latest stable version that the metadata will be generated, which is depended on the global variables majorVersion and minorVersion
        outputFileType : str
            output file extension (type) that we want to create, default value is `yaml` if no value is given for that parameter
        ...
        Returns
        ----------
        None
        """
        with open(f'{outputMetadataPath}/{fileName}/{fileName}%{self.stableVersion}.{outputFileType}', 'w') as file:
            yaml.dump(schema, file, sort_keys=False, version=(self.majorVersion, self.minorVersion))
            