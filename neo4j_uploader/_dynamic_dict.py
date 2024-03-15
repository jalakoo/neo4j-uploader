from dataclasses import dataclass
import copy
import logging

@dataclass
class DynamicDict:
    def __init__(self, data: dict = {}):
        self.data = copy.deepcopy(data)

    def create_nested_dict(self, key_list, new_value):
        # Creates a nested dictionary from a list of keys and a value
        if not key_list:
            return new_value
        else:
            key, *rest_keys = key_list
            return {key: self.create_nested_dict(rest_keys, new_value)}
    
    def recursive_update(self, existing_dict, new_dict):
        # Updates a nested dictionary with a source dictionary
        # If a key in new_dict is a nested dictionary, it recursively updates the corresponding nested dictionary in existing_dict
        for key, value in new_dict.items():
            if isinstance(value, dict):
                # If the value is a dictionary, recursively update the nested dictionary
                existing_dict[key] = self.recursive_update(existing_dict.get(key, {}), value)
            else:
                # If it's not a dictionary, update the key's value
                existing_dict[key] = value
        return existing_dict

    def getval(self, keys: list[str]) -> any:
        """
        Get value from the internal dict structure from a list of keys

        Args:
            keys: List of string keys

        Returns:
            The value from the internal dictionary from the keys paths provided in the keys arg.
            Returns None if any of the keys are not found.

        """
        if not keys:
            return None
        if len(keys) == 0:
            return None

        data = self.data
        try:
            for k in keys:
                # The source record does not contain a value at the specified path
                # if not isinstance(data, dict) and not isinstance(data, list):
                #     return None

                try:
                    if isinstance(data, list):
                        k = int(k)
                    data = data[k]
                except Exception as e:
                    logging.error(f'Problem getting value from path {keys} in data {data}')
                    return None
            return data
        except KeyError:
            return None

    def setval(self, keys: list, value: any):
        """
        Sets value to the internal dict structure from a list of keys

        Args:
            keys: A (list[str], any) tuple consisting of the ([list of keys representing key path], the value to add)

        """
        new_dict = self.create_nested_dict(keys, value)

        # combined_dict = {**self.data, **new_dict}
        combined_dict = self.recursive_update(self.data, new_dict)
        self.data = combined_dict

    def to_dict(self):
        return self.data