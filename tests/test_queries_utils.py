from neo4j_uploader._queries_utils import value_for_keypath, keypath_to_1st_list, keypath_list_to_1st_list, does_keypath_contain_list

record = {
    'a': {
        'b': {
            'c': 123
        }
    }
}

flat_record = {
    'a': 123
}

record_with_list = {
    'a': [
        {
            'b': 123
        },
        {
            'b': 456
        }
    ]
}

class TestRecordWithListValueForKeyPath():
    def test_valid_value_for_keypath(self):
        # Test case 1: Valid keypath
        path = 'a.0.b'
        expected_value = 123
        assert value_for_keypath(path, record_with_list) == expected_value 

    def test_invalid_value_for_keypath(self):
        # Test case 2: Invalid target key
        path = 'a.0.d'
        expected_value = None
        assert value_for_keypath(path, record) == expected_value

        # Test invalid list index
        path = 'a.2.b'
        expected_value = None
        assert value_for_keypath(path, record) == expected_value

class TestFlatRecordValueForKeypath():
    def test_valid_value_for_keypath(self):
        # Test case 1: Valid keypath
        path = 'a'
        expected_value = 123
        assert value_for_keypath(path, flat_record) == expected_value

    def test_invalid_value_for_keypath(self):
        # Test case 2: Invalid target key
        path = 'b'
        expected_value = None
        assert value_for_keypath(path, record) == expected_value


class TestDoesKeypathContainsList():
    def test_does_keypath_contains_list_with_list(self):
        record = {
            'a': {
                'b': {
                    'c': [1, 2, 3]
                }
            }
        }
        path = 'a.b.c'
        result = does_keypath_contain_list(path, record)
        assert result is True

    def test_does_keypath_contains_list_with_list_2(self):
        record =  {
                    'from': 'a', 
                    'to': [
                        {
                            'gid':'x'
                        },
                        {
                            'gid':'y'
                        }
                    ]
                    }
        path = 'to.gid'
        result = does_keypath_contain_list(path, record)
        assert result is True

    def test_does_keypath_contains_list_without_list(self):
        record = {
            'a': {
                'b': {
                    'c': 'value'
                }
            }
        }
        path = 'a.b.c'
        result = does_keypath_contain_list(path, record)
        assert result is False

    def test_does_keypath_contains_list_nested_list(self):
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
        result = does_keypath_contain_list(path, record)
        assert result is True

    def test_does_keypath_contains_list_empty_record(self):
        record = {}
        path = 'a.b.c'
        result = does_keypath_contain_list(path, record)
        assert result is False

class TestRecordValueForKeypath():
    def test_valid_value_for_keypath(self):
        # Test case 1: Valid keypath
        path = 'a.b.c'
        expected_value = 123
        assert value_for_keypath(path, record) == expected_value
        
    def test_invalid_value_for_keypath(self):
        # Test case 2: Invalid keypath
        path = 'a.b.d'
        expected_value = None
        assert value_for_keypath(path, record) == expected_value

    def test_empty_value_for_keypath(self):
        # Test case 3: Empty keypath
        path = ''
        expected_value = None
        assert value_for_keypath(path, record) == expected_value

    def test_invalid_value_for_keypath(self):
        # Test case 4: Key not found
        path = 'x.y.z'
        expected_value = None
        assert value_for_keypath(path, record) == expected_value
    

class TestKeypathListTo1stList():
    def test_keypath_list_to_1st_list_with_list(self):
        record = {
            'a': [
                {
                    'b':'c'
                }
            ]
        }
        path = 'a.b'
        result = keypath_list_to_1st_list(path, record)
        assert result == ['a']

        record = {
            'a': {
                'b': {
                    'c': [1, 2, 3]
                }
            }
        }
        path = 'a.b.c'
        result = keypath_list_to_1st_list(path, record)
        assert result == ['a', 'b', 'c']

    def test_keypath_list_to_1st_list_without_list(self):
        record = {
            'a': {
                'b': {
                    'c': 'value'
                }
            }
        }
        path = 'a.b.c'
        result = keypath_list_to_1st_list(path, record)
        assert result is None

    def test_keypath_list_to_1st_list_nested_list(self):
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
        result = keypath_list_to_1st_list(path, record)
        assert result == ['a', 'b', 'c']

    def test_keypath_list_to_1st_list_empty_record(self):
        record = {}
        path = 'a.b.c'
        result = keypath_list_to_1st_list(path, record)
        assert result is None

class TestKeypathTo1stList():

    def test_keypath_to_1st_list_with_list(self):
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

        record = {
            'a': [
                {
                    "b":"c"
                }
            ]
        }
        path = 'a.b'
        result = keypath_to_1st_list(path, record)
        assert result == 'a'

    def test_keypath_to_1st_list_without_list(self):
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

    def test_keypath_to_1st_list_nested_list(self):
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

    def test_keypath_to_1st_list_empty_record(self):
        record = {}
        path = 'a.b.c'
        result = keypath_to_1st_list(path, record)
        assert result is None