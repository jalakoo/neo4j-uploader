from neo4j_uploader._queries_utils import keypath_to_1st_list

def test_keypath_to_1st_list_with_list():
    record = {
        'a': {
            'b': {
                'c': [1, 2, 3]
            }
        }
    }
    path = 'a.b.c'
    result = keypath_to_1st_list(path, record)
    assert result == 'a.b.c'

def test_keypath_to_1st_list_without_list():
    record = {
        'a': {
            'b': {
                'c': 'value'
            }
        }
    }
    path = 'a.b.c'
    result = keypath_to_1st_list(path, record)
    assert result is None

def test_keypath_to_1st_list_nested_list():
    record = {
        'a': {
            'b': {
                'c': [
                    {'d': 1},
                    {'d': 2},
                    {'d': 3}
                ]
            }
        }
    }
    path = 'a.b.c.d'
    result = keypath_to_1st_list(path, record)
    assert result == 'a.b.c'

def test_keypath_to_1st_list_empty_record():
    record = {}
    path = 'a.b.c'
    result = keypath_to_1st_list(path, record)
    assert result is None