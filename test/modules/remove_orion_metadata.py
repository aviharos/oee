def remove_orion_metadata(obj: dict):
    """Remove Orion metadata fields from an object downloaded from Orion

    Works recursively.

    Args:
        obj (dict): Orion object

    Returns:
        Orion object without metadata fields (dict)
    """
    if isinstance(obj, dict):
        # build a new dict
        new_dict = {}
        for key in obj.keys():
            if key == 'metadata':
                pass
            else:
                new_dict[key] = remove_orion_metadata(obj[key])
        return new_dict
    elif isinstance(obj, list):
        return [remove_orion_metadata(x) for x in obj]
    else:
        return obj

