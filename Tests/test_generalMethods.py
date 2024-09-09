# from PySparkGenerator.PySparkParallelGeneratorSpeed import generalMethods

def test_generalMethods() -> None:
    generalMethods = generalMethods(0, 0)
    assert generalMethods.stableVersion == "0.0"