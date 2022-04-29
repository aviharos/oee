# -*- coding: utf-8 -*-
# Standard Library imports
# PyPI packages
import requests

# custom imports
from conf import conf

def getObjectFromOrion(object_id, headers=None, data='options=keyValues', host=conf['orion_host'], port=conf['orion_port']):
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

def getWorkstationIdsFromOrion():
    #return workstationIds
    pass

def getActiveJobId(workstationId):
    # return jobId
    pass

def testgetObjectFromOrion():
    status_code, OperatorSchedule = getObjectFromOrion('urn:ngsi_ld:OperatorSchedule:1')
    print(status_code)
    print(OperatorSchedule)

if __name__ == '__main__':
    testgetObjectFromOrion()
