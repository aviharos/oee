# -*- coding: utf-8 -*-
# Standard Library imports
# PyPI packages
import json
import requests

# custom imports
from conf import conf

def getObject(object_id, host=conf['orion_host'], port=conf['orion_port']):
    '''
    Returns the object in JSON format idenfitied by object_id and the status code of the request
    '''
    url = f'http://{host}:{port}/v2/entities/{object_id}'
    try:
        response = requests.get(url)
        response.close()
        return response.status_code, response.json()
    except:
        raise RuntimeError(f'Get request failed to URL: {url}')

def getWorkstationIds():
    #return workstationIds
    pass

def getActiveJobId(workstationId):
    # return jobId
    pass

def testgetObject():
    status_code, parsedJob = getObject('urn:ngsi_ld:Job:202200045')
    print(status_code)
    print(parsedJob['id'])
    print(parsedJob['GoodPartCounter']['value'])

if __name__ == '__main__':
    testgetObject()
