# -*- coding: utf-8 -*-
# Standard Library imports
# PyPI packages
import logging
import requests
import sys

# custom imports
from conf import conf

logging_levels = {'DEBUG': logging.DEBUG,
                  'INFO': logging.INFO,
                  'WARNING': logging.WARNING,
                  'ERROR': logging.ERROR,
                  'CRITICAL': logging.CRITICAL}
logger_Orion = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
logger_Orion.setLevel(logging_levels[conf['logging_level']])
if conf['log_to_file']:
    file_handler_Orion = logging.FileHandler('Orion.log')
    file_handler_Orion.setFormatter(formatter)
    logger_Orion.addHandler(file_handler_Orion)
if conf['log_to_stdout']:
    stream_handler_Orion = logging.StreamHandler(sys.stdout)
    stream_handler_Orion.setFormatter(formatter)
    logger_Orion.addHandler(stream_handler_Orion)


def getRequestToOrion(url):
    try:
        response = requests.get(url)
        response.close()
    except:
        logger_Orion.error(f'Get request failed to URL: {url} ')
        return None, None
    else:
        if response.status_code == 200:
            return response.status_code, response.json()
        else:
            logger_Orion.error(f'Get request failed to URL: {url}, status code:{response.status_code}')
            return response.status_code, None


def getObject(object_id, host=conf['orion_host'], port=conf['orion_port']):
    '''
    Returns the object in JSON format idenfitied by object_id and the status code of the request
    '''
    url = f'http://{host}:{port}/v2/entities/{object_id}'
    return getRequestToOrion(url)


def getWorkstationIds():
    url = f'http://{conf["orion_host"]}:{conf["orion_port"]}/v2/entities?type=Workstation'
    status_code, entities = getRequestToOrion(url)
    workstation_ids = []
    if status_code != 200:
        return status_code, workstation_ids
    for ws in entities:
        workstation_ids.append(ws['id'])
    return status_code, workstation_ids


def getActiveJobId(workstationId):
    status_code, workstation = getObject(workstationId)
    if status_code != 200:
        return status_code, workstation
    try:
        refJobId = workstation['RefJob']['value']
        return status_code, refJobId
    except KeyError:
        logger_Orion.error(f'Missing RefJob attribute in Workstation: {workstation}, no OEE data')
        return 200, None


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
