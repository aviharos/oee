# -*- coding: utf-8 -*-
# Standard Library imports
# PyPI packages
import requests

# custom imports
from conf import conf

def getRequest(url):
    try:
        response = requests.get(url)
        response.close()
    except Exception as error:
        raise RuntimeError(f'Get request failed to URL: {url}.\n{error}')

    else:
        try:
            response.json()
        except requests.exceptions.JSONDecodeError:
            raise ValueError(f'The JSON could not be decoded after GET request to {url}. Response:\n{response}')
        return response.status_code, response.json()

def get(object_id, host=conf['orion_host'], port=conf['orion_port']):
    '''
    Returns the object in JSON format idenfitied by object_id and the status code of the request
    '''
    url = f'http://{host}:{port}/v2/entities/{object_id}'
    status_code, json_ = getRequest(url)
    if status_code != 200:
        raise RuntimeError(f'Failed to get object from Orion broker:{object_id}, status_code:{status_code}; no OEE data')
    return json_

def exists(object_id):
    try:
        get(object_id)
        return True
    except RuntimeError:
        return False

def getWorkstations():
    url = f'http://{conf["orion_host"]}:{conf["orion_port"]}/v2/entities?type=Workstation'
    status_code, workstations = getRequest(url)
    if status_code != 200:
        raise RuntimeError(f'Critical: could not get Workstations from Orion with GET request to URL: {url}')
    return workstations

def update(objects):
    '''
    A method that takes an iterable (objects) that contains Orion objects,
    then updates them in Orion.
    If an object already exists, it will be overwritten. More information:
    https://github.com/FIWARE/tutorials.CRUD-Operations#six-request
    '''
    url = f'http://conf["orion_host"]:conf["orion_port"]/v2/op/update'
    try:
        json_ = {'actionType': 'append',
                'entities': list(objects)}
    except TypeError:
        raise TypeError(f'The objects {objects} are not iterable, cannot make a list. Please, provide an iterable object')
    response = requests.post(url, json=json_)
    if response.status_code != 204:
        raise RuntimeError(f'Failed to update objects in Orion.\nStatus_code: {response.status_code}\nObjects:\n{objects}')
    else:
        return response.status_code

# def getActiveJobId(workstationId):
#     workstation = get(workstationId)
#     try:
#         refJobId = workstation['RefJob']['value']
#         return refJobId
#     except KeyError:
#         raise KeyError(f'Missing RefJob attribute in Workstation: {workstation}, no OEE data')

# def post(url, obj):
#     try:
#         response = requests.post(url, json=obj)
#         response.close()
#     except:
#         raise RuntimeError(f'Put request failed to URL: {url} for unknown reason')
#
#     else:
#         if response.status_code == 201:
#             return response.status_code
#         else:
#             raise RuntimeError(f'The object could not be created in Orion. URL: {url}, status code:{response.status_code}')

# def deleteObject(url):
#     try:
#         response = requests.delete(url)
#         response.close()
#     except:
#         raise RuntimeError(f'Delete request failed to URL: {url} for unknown reason')
#     else:
#         if response.status_code == 204:
#             return response.status_code
#         else:
#             raise RuntimeError(f'The object could not be deleted in Orion. URL: {url}, status code:{response.status_code}')

