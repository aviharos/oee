# -*- coding: utf-8 -*-
"""A module for creating templates of objects stored in JSON
"""
# Standard Library imports
import json


def object_to_template(file_: str):
    """A module for creating templates of Orion objects stored in the "jsons" directory

    Reads a json object from the json folder,
    then sets its ID and all attribute values to None,
    and returns the blank template.
    Does not consider nested attributes.
    The first level attribute is cleared.

    Args:
        file_: the path to the json file containing the Orion object

    Returns:
        the template Orion object as a dict
    """
    with open(file_, "r") as f:
        obj = json.load(f)
    for key in obj.keys():
        if key == "id":
            obj[key] = None
        elif key in ["type", "i40AssetType", "i40ProcessType", "i40RecipeType"]:
            pass
        else:
            obj[key]["value"] = None
    return obj
