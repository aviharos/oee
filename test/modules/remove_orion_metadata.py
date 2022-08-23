def remove_orion_metadata(obj):
    if isinstance(obj, dict):
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

