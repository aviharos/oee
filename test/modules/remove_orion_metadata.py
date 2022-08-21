def remove_orion_metadata(json_):
    if isinstance(json_, dict):
        new_dict = {}
        for key in json_.keys():
            if key == 'metadata':
                pass
            else:
                new_dict[key] = remove_orion_metadata(json_[key])
        return new_dict
    elif isinstance(json_, list):
        return [remove_orion_metadata(x) for x in json_]
    else:
        return json_

