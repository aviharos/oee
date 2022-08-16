# -*- coding: utf-8 -*-
# Standard Library imports
# PyPI packages
import requests

# custom imports
from conf import conf
from Logger import getLogger

logger_Orion = getLogger(__name__)


def getRequestToOrion(url):
    try:
        response = requests.get(url)
        response.close()
    except:
        raise RuntimeError(f'Get request failed to URL: {url} for unknown reason')

    else:
        if response.status_code == 200:
            return response.json()
        else:
            raise RuntimeError(f'Get request failed to URL: {url}, status code:{response.status_code}')


def getObject(object_id, host=conf['orion_host'], port=conf['orion_port']):
    '''
    Returns the object in JSON format idenfitied by object_id and the status code of the request
    '''
    url = f'http://{host}:{port}/v2/entities/{object_id}'
    return getRequestToOrion(url)


def getWorkstationIds():
    url = f'http://{conf["orion_host"]}:{conf["orion_port"]}/v2/entities?type=Workstation'
    entities = getRequestToOrion(url)
    workstation_ids = []
    for ws in entities:
        workstation_ids.append(ws['id'])
    return workstation_ids


def getActiveJobId(workstationId):
    workstation = getObject(workstationId)
    try:
        refJobId = workstation['RefJob']['value']
        return refJobId
    except KeyError:
        raise KeyError(f'Missing RefJob attribute in Workstation: {workstation}, no OEE data')

def postObjectToOrion(url, obj):
    try:
        response = requests.post(url, json=obj)
        response.close()
    except:
        raise RuntimeError(f'Put request failed to URL: {url} for unknown reason')

    else:
        if response.status_code == 201:
            return response.json()
        else:
            raise RuntimeError(f'The object could not be created in Orion. URL: {url}, status code:{response.status_code}')

def deleteObject(url):
    try:
        response = requests.delete(url)
        response.close()
    except:
        raise RuntimeError(f'Delete request failed to URL: {url} for unknown reason')

    else:
        if response.status_code == 204:
            return response.status_code
        else:
            raise RuntimeError(f'The object could not be deleted in Orion. URL: {url}, status code:{response.status_code}')

def test_getObject():
    status_code, parsedJob = getObject('urn:ngsi_ld:Job:202200045')
    logger_Orion.debug(f'status_code: {status_code}')
    logger_Orion.debug(f'parsed jobId: {parsedJob["id"]}')
    logger_Orion.debug(f'parsed job\'s GoodPartCounter: {parsedJob["GoodPartCounter"]["value"]}')


def test_getworkstationIds():
    status_code, workstation_ids = getWorkstationIds()
    logger_Orion.debug(f'Workstation ids: {workstation_ids}')


def test_getActiveJobId():
    status_code1, workstation_ids = getWorkstationIds()
    for wsid in workstation_ids:
        status_code2, jobId = getActiveJobId(wsid)
        logger_Orion.debug(f'Workstation id: {wsid}, Job id: {jobId}')

if __name__ == '__main__':
    # test_getObject()
    test_getworkstationIds()
    test_getActiveJobId()
