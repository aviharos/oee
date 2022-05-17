# -*- coding: utf-8 -*-
# Standard Library imports
# PyPI packages
import logging
import requests

# custom imports
from conf import conf

log_levels={'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL}
logger_Orion = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
logger_Orion.setLevel(log_levels[conf['logging_level']])
file_handler = logging.FileHandler('Orion.log')
file_handler.setFormatter(formatter)
logger_Orion.addHandler(file_handler)

def getObject(object_id, host=conf['orion_host'], port=conf['orion_port']):
    '''
    Returns the object in JSON format idenfitied by object_id and the status code of the request
    '''
    url = f'http://{host}:{port}/v2/entities/{object_id}'
    try:
        response = requests.get(url)
        response.close()
    except:
        logger_Orion.error(f'Get request failed to URL for unknown reason: {url}')
        return None, None
    else:
        if response.status_code == 200:
            return response.status_code, response.json()
        else:
            logger_Orion.error(f'Get request failed to URL: {url}, status code:{response.status_code}')
            return response.status_code, None

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
