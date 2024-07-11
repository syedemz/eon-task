import sys
import os
import pytest
import pandas as pd
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
new_path = SCRIPT_DIR.rsplit("/tests", 1)[0]
sys.path.append(new_path)
new_path = new_path + '/en-file-processor-lambda'
sys.path.append(new_path)

print("first one")
print(sys.path)
from fileProcessor import validate_json_object, validate_timestamp



invalid_data_missing_key = pd.DataFrame({
        "timestamp": "2024-01-01T13:15:00.000Z",
        "iotreadings": {
            "value1": 13,
            "value2": 220,
            "value3": 110
        }
    })


invalid_timestamp = "2024-01-01T13:15:00.Z"

def test_missing_key():
    assert validate_json_object(invalid_data_missing_key) == False

def test_invalid_timestamp():
    assert validate_timestamp(invalid_timestamp) == False