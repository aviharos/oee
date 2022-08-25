# -*- coding: utf-8 -*-
# Standard Library imports
# PyPI packages
import os
import requests

from modules.log_it import log_it
from Logger import getLogger

logger_Orion = getLogger(__name__)

# get environment variables
ORION_HOST = os.environ.get("ORION_HOST")
if ORION_HOST is None:
    raise RuntimeError("Critical: ORION_HOST environment variable is not set")

ORION_PORT = os.environ.get("ORION_PORT")
if ORION_PORT is None:
    logger_Orion.warning(
        "ORION_PORT environment variable not set, using default value: 1026"
    )
    ORION_PORT = 1026


def getRequest(url):
    try:
        response = requests.get(url)
        response.close()
    except Exception as error:
        raise RuntimeError(f"Get request failed to URL: {url}") from error

    else:
        try:
            response.json()
        except requests.exceptions.JSONDecodeError as error:
            raise ValueError(
                f"The JSON could not be decoded after GET request to {url}. Response:\n{response}"
            ) from error
        return response.status_code, response.json()


def get(object_id, host=ORION_HOST, port=ORION_PORT):
    """
    Returns the object in JSON format idenfitied by object_id and the status code of the request
    """
    url = f"http://{host}:{port}/v2/entities/{object_id}"
    logger_Orion.debug(url)
    status_code, json_ = getRequest(url)
    if status_code != 200:
        raise RuntimeError(
            f"Failed to get object from Orion broker:{object_id}, status_code:{status_code}; no OEE data"
        )
    return json_


def exists(object_id):
    try:
        get(object_id)
        return True
    except RuntimeError:
        return False


def getWorkstations():
    url = f"http://{ORION_HOST}:{ORION_PORT}/v2/entities?type=Workstation"
    status_code, workstations = getRequest(url)
    if status_code != 200:
        raise RuntimeError(
            f"Critical: could not get Workstations from Orion with GET request to URL: {url}"
        )
    return workstations


def update(objects):
    """
    A method that takes an iterable (objects) that contains Orion objects,
    then updates them in Orion.
    If an object already exists, it will be overwritten. More information:
    https://github.com/FIWARE/tutorials.CRUD-Operations#six-request
    """
    url = f"http://{ORION_HOST}:{ORION_PORT}/v2/op/update"
    try:
        json_ = {"actionType": "append", "entities": list(objects)}
    except TypeError as error:
        raise TypeError(
            f"The objects {objects} are not iterable, cannot make a list. Please, provide an iterable object"
        ) from error
    response = requests.post(url, json=json_)
    if response.status_code != 204:
        raise RuntimeError(
            f"Failed to update objects in Orion.\nStatus_code: {response.status_code}\nObjects:\n{objects}"
        )
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
