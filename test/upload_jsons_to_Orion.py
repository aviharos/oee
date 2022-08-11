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
        obj = json.loads(json_)
        Orion.postObjectToOrion(url, obj)

if __name__ == '__main__':
    main()

