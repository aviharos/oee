'''
This file is meant to upload the json objects to the
Orion broker before testing with a physical or virtual PLC.
'''

import sys
import glob
import json

# PyPI imports

# Custom imports
sys.path.insert(0, '../app')
import Orion
from conf import conf

def main():
    url = f'http://{conf["orion_host"]}:{conf["orion_port"]}/v2/entities'
    jsons = glob.glob('../json/*.json')
    for json_ in jsons:
        print(json_)
        with open(json_, 'r') as f:
            obj = json.load(f)
        obj_url = f'http://{conf["orion_host"]}:{conf["orion_port"]}/v2/entities/{obj["id"]}'
        try:
            Orion.deleteObject(obj_url)
        except RuntimeError:
            pass
        Orion.postObjectToOrion(url, obj)

if __name__ == '__main__':
    main()

