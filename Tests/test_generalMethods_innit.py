import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PySparkGenerator')))
import pytest

from GeneralMethods import *

@pytest.mark.parametrize(
    "major_version, minor_version, expected_major, expected_minor, expected_stable",
    [
        (None, None, 0, 0, 0.0),
        (0, 0, 0, 0, 0.0),
        (1, 2, 1, 2, 1.2),
        (-1, 2, 1, 2, 1.2),
        (1, -2, 1, 2, 1.2),
        (-1, -2, 1, 2, 1.2),
    ]
)

def test_general_methods_innit(major_version, minor_version, expected_major, expected_minor, expected_stable) -> None:
    if major_version is None and minor_version is None:
        instace_general_methods = generalMethods()
    else:
        instace_general_methods = generalMethods(major_version, minor_version)

    assert instace_general_methods.majorVersion == expected_major
    assert instace_general_methods.minorVersion == expected_minor
    assert float(instace_general_methods.stableVersion) == expected_stable
