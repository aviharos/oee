import json


def object_to_template(file_):
    """
    Reads a json object from the json folder,
    then sets its ID and all attribute values to None,
    and returns the blank template.
    Does not consider nested attributes.
    The first level attribute is cleared.
    """
    with open(file_, "r") as f:
        obj = json.load(f)
    for key in obj.keys():
        if key == "id":
            obj[key] = None
        elif key == "type":
            pass
        else:
            obj[key]["value"] = None
    return obj
