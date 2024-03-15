from neo4j_uploader._dynamic_dict import DynamicDict

def test_getval_single_key():
    data = {
        "key1": "value1",
        "key2": "value2",
        "key3": "value3"
    }
    dynamic_dict = DynamicDict(data)
    result = dynamic_dict.getval(["key1"])
    assert result == "value1"

def test_getval_from_list():
    data = {
        "key1": [
            "value1", "value2"
        ]
    }
    dynamic_dict = DynamicDict(data)
    result = dynamic_dict.getval(["key1",1])
    assert result == "value2"


def test_getval_from_list_of_dicts():
    data = {
        "key1": [
            {"key2":"value1"}
        ]
    }
    dynamic_dict = DynamicDict(data)
    result = dynamic_dict.getval(["key1",0,"key2"])
    assert result == "value1"

def test_getval_multiple_keys():
    data = {
        "key1": {
            "key2": {
                "key3": "value3"
            }
        }
    }
    dynamic_dict = DynamicDict(data)
    result = dynamic_dict.getval(["key1", "key2", "key3"])
    assert result == "value3"

def test_getval_invalid_key():
    data = {
        "key1": "value1"
    }
    dynamic_dict = DynamicDict(data)
    result = dynamic_dict.getval(["invalid_key"])
    assert result is None

def test_getval_empty_keys():
    data = {
        "key1": "value1"
    }
    dynamic_dict = DynamicDict(data)
    result = dynamic_dict.getval([])
    assert result is None