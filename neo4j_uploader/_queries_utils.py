from enum import Enum
from copy import deepcopy
from neo4j_uploader._dynamic_dict import DynamicDict

class ElementType(Enum):
    UNKNOWN = 0
    NODE = 1
    RELATIONSHIP = 2

def convert_to_hashable(obj):
    if isinstance(obj, dict):
        return tuple({k: convert_to_hashable(v) for k, v in obj.items()}.items())
    elif isinstance(obj, list):
        return tuple(convert_to_hashable(i) for i in obj)
    else:
        return obj
    
def deduped(data: list[any])-> list[any]:
    unique = [] 
    seen = set()

    for d in data:
        # Deepcopy to avoid modifying original object
        tmp = deepcopy(d)
        # Convert nested dicts and lists to tuples for hashing
        tmp = convert_to_hashable(tmp)
        if isinstance(tmp, tuple):
            t = tmp
        else:
            t = tuple(tmp.items())
        
        if t not in seen:
            unique.append(d)
            seen.add(t)
    
    return unique
    

def value_for_keypath(
        path: str,
        record: dict
    )-> any:
        path_list = path.split(".")
        dynamic_record = DynamicDict(record)
        value = dynamic_record.getval(path_list)
        return value

def keypath_list_to_1st_list(
        path: str,
        record: dict
    )->list[str]:
    path_list = path.split(".")
    dynamic_record = DynamicDict(record)
    check_path = []     
    for path in path_list:
        check_path.append(path)
        value = dynamic_record.getval(check_path)
        if isinstance(value, list):
            return check_path
    return None
        
def keypath_to_1st_list(
        path: str,
        record: dict
    )->str:
    path_list = keypath_list_to_1st_list(path, record)
    if path_list is None:
        return None
    return ".".join(path_list)

def does_keypath_contain_list(
        path: str,
        record: dict
    )-> bool:
        list_path = keypath_to_1st_list(path, record)
        if list_path is None:
            return False
        return len(list_path) > 0

def properties(
        suffix: str,
        record: dict,
        exclude_keys: list[str] = []
    ) -> (str, dict):

    # Sample string output
    # " {`age`:$age_test_0, `name`:$name_test_0}"

    # Sample dict output
    # {
    #   "age_test_0": 30,
    #   "name_test_0": "John Wick"
    # }

    # Convert each batch of records
    result_params = {}
    
    # Filter out unwanted keys
    filtered_keys = [key for key in record.keys() if key not in exclude_keys]

    # Sort keys for consistent testing
    sorted_keys = sorted(list(filtered_keys))

    query = " {"
    for k_idx, a_key in enumerate(sorted_keys):

        value = record[a_key]

        # Do not set properties with a None/Null/Empty value
        if value is None:
            continue
        if isinstance(value, str):
            if value.lower() == "none":
                continue
            if value.lower() == "null":
                continue
            if value.lower() == "empty":
                continue
            if value.lower() == "":
                continue

        # Nested dicts not supported in Neo4j properties. Lists with dictionaries may cause issues. Stringify both for now
        if isinstance(value, dict) or isinstance(value, list):
            value = str(value)

        # Prefix multiple items in Cypher with comma
        if k_idx != 0:
            query += ", "

        # Add params for query
        param_key = f'{a_key}_{suffix}'
        result_params[param_key] = value

        # Add string representation of property data
        query += f'`{a_key}`:${param_key}'

    # Close out query
    query += "}"


    return (query, result_params)