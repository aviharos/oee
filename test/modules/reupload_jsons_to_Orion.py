'''
This file is meant to upload the json objects to the
Orion broker before testing with a physical or virtual PLC.
'''

import json
import glob
import sys

# PyPI imports

# Custom imports
sys.path.insert(0, '../app')
import Orion

def main():
    jsons = glob.glob('../json/*.json')
    objects = []
    for json_ in jsons:
        with open(json_, 'r') as f:
            obj = json.load(f)
        objects.append(obj)
    Orion.update(objects)

if __name__ == '__main__':
    main()

