
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
