# -*- coding: utf-8 -*-
# Standard Library imports
from datetime import datetime
import sys
import time

# PyPI packages
import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.types import DateTime, Float, BigInteger, Text

# custom imports
from conf import conf
from Logger import getLogger
import Orion

global engine, con
global conf, postgresSchema, day

logger_OEE = getLogger(__name__)

postgresSchema = conf['postgresSchema']
DATETIME_FORMAT='%Y-%m-%d %H:%M:%S.%f'
col_dtypes={'recvtimets': BigInteger(),
            'recvtime': DateTime(),
            'availability': Float(),
            'performance': Float(),
            'quality': Float(),
            'oee': Float(),
            'throughput_shift': Float(),
            'jobs': Text()}


def msToDateTimeString(ms):
    return str(datetime.fromtimestamp(ms/1000.0).strftime(DATETIME_FORMAT))[:-3]


def stringToDateTime(string):
    return datetime.strptime(string, DATETIME_FORMAT)


def find_operation(part, operationType):
    for operation in part['Operations']['value']:
        if operation['OperationType']['value'] == operationType:
            return operation
    raise AttributeError(f'The part {part} has no operation with type {operationType}')


def calculateOEE(workstationId, jobId, _time_override=None):
    # _time_override will never be used in production, we will work with the current time
    now = datetime.now()
    if _time_override is not None:
        now = _time_override
        logger_OEE.warning(f'Time override in calculateOEE: {_time_override}')
    
    #integer unix time in milliseconds at 00:00:00
    timeTodayStart = int(stringToDateTime(str(now.date()) + ' 00:00:00.000').timestamp()*1000)
    
    today = now.date()
    now_unix = now.timestamp()*1000
    status_code, sch_json = Orion.getObject('urn:ngsi_ld:OperatorSchedule:1')
    if status_code != 200:
        logger_OEE.error(f'Failed to get object from Orion broker:urn:ngsi_ld:OperatorSchedule:1, status_code:{status_code}')
        return None
    try:
        OperatorScheduleStartsAt = datetime.strptime(str(now.date())+ ' ' + str(sch_json['OperatorWorkingScheduleStartsAt']['value']), '%Y-%m-%d %H:%M:%S')        
        OperatorScheduleStopsAt = datetime.strptime(str(now.date())+ ' ' + str(sch_json['OperatorWorkingScheduleStopsAt']['value']), '%Y-%m-%d %H:%M:%S')
    except (ValueError, KeyError) as error:
        logger_OEE.error(f'Invalid time format in OperatorSchedule. Traceback: {error}')
        return None
    
    if now < OperatorScheduleStartsAt:
        logger_OEE.info(f'The shift has not started yet before time: {now}, no OEE data')
        return None
    if now > OperatorScheduleStopsAt:
        logger_OEE.info(f'The current time: {now} is after the shift\'s end, no OEE data')
        return None

    # availability        
    workstation = workstationId.replace(':', '_').lower() + '_workstation'
    # TODO change to bigint in production
    try:
        df = pd.read_sql_query(f'''select * from {postgresSchema}.{workstation}
                               where {timeTodayStart} < cast (recvtimets as numeric(14,1))
                               and cast (recvtimets as numeric(14,1)) <= {now_unix};''',con=con)
    except (psycopg2.errors.UndefinedTable,
            sqlalchemy.exc.ProgrammingError) as error:
        logger_OEE.error(f'The SQL table: {workstation} does not exist within the schema: {postgresSchema}')
        return None

    df['recvtimets'] = df['recvtimets'].map(float).map(int)
    #df = df[(timeTodayStart < df.recvtimets) & (df.recvtimets <= now_unix)]

    if df.size == 0:   
        logger_OEE.warning(f'No workstation data found for {workstationId} up to time {now} on day {today}, no OEE data')
        return None

    # Available is true and false in this periodical order, starting with true
    # we can sum the timestamps of the true values and the false values disctinctly, getting 2 sums
    # the total available time is their difference
    # TODO availability df
    available_true = df[df.attrvalue == 'true']
    available_false = df[df.attrvalue == 'false']
    total_available_time = available_false['recvtimets'].sum() - available_true['recvtimets'].sum()
    # if the Workstation is available currently, we need to add
    # the current timestamp to the true timestamps' sum
    if (df.iloc[-1].attrvalue =='true'):
        total_available_time += now_unix
    
    total_available_time_hours = time.strftime("%H:%M:%S", time.gmtime(total_available_time/1000))
    
    logger_OEE.info(f'Total available time: {total_available_time_hours}')
        
    availability = total_available_time/(now.timestamp()*1000-OperatorScheduleStartsAt.timestamp()*1000)
    
    # performance, quality
    job = jobId.replace(':', '_').lower() + '_job'
    # TODO change to bigint in production
    df = pd.read_sql_query(f'''select * from default_service.{job}
                           where {timeTodayStart} < cast (recvtimets as numeric(14,1))
                           and cast (recvtimets as numeric(14,1)) <= {now_unix};''',con=con)
    df['recvtimets'] = df['recvtimets'].map(float).map(int)
    #df = df[(timeTodayStart < df.recvtimets) & (df.recvtimets <= now_unix)]

    if df.size == 0:
        logger_OEE.warning(f'No job data found for {jobId} up to time {now} on day {today}.')
        return None
    
    n_successful_mouldings = len(df[df.attrname == 'GoodPartCounter']['attrvalue'].unique())
    n_failed_mouldings = len(df[df.attrname == 'RejectPartCounter']['attrvalue'].unique())
    n_total_mouldings = n_successful_mouldings + n_failed_mouldings
    
    status_code, job_json = Orion.getObject(jobId)
    if status_code != 200:
        logger_OEE.error(f'Failed to get object from Orion broker:{jobId}, status_code:{status_code}; no OEE data')
        return None

    partId = job_json['RefPart']['value']

    status_code, part_json = Orion.getObject(partId)
    if status_code != 200:
        logger_OEE.error(f'Failed to get object from Orion broker:{partId}, status_code:{status_code}; no OEE data')
        return None

    current_operation_type = job_json['CurrentOperationType']['value']
    operation = find_operation(part_json, current_operation_type)
    operationTime = operation['OperationTime']['value']
    partsPerOperation = operation['PartsPerOperation']['value']

    performance = n_total_mouldings * operationTime / total_available_time
    quality = n_successful_mouldings / n_total_mouldings
    oee = availability * performance * quality

    shiftLengthInMilliseconds = OperatorScheduleStopsAt.timestamp()*1000 - OperatorScheduleStartsAt.timestamp()*1000
    throughput = (shiftLengthInMilliseconds / operationTime) * partsPerOperation * oee

    return availability, performance, quality, oee, throughput


def insertOEE(workstationId, availability, performance, quality, oee, throughput, jobIds, _time_override=None):
    table_name = workstationId.replace(':', '_').lower() + '_workstation_oee'
    now = datetime.now()
    if _time_override is not None:
        now = _time_override
        logger_OEE.warning(f'Time override in insertOEE: {_time_override}')
    now_unix = now.timestamp()*1000
    oeeData = pd.DataFrame.from_dict({'recvtimets': [now_unix],
                                      'recvtime': [msToDateTimeString(now_unix)],
                                      'availability': [availability],
                                      'performance': [performance],
                                      'quality': [quality],
                                      'oee': [oee],
                                      'throughput_shift': [throughput],
                                      'jobs': [jobIds]})
    oeeData.to_sql(name=table_name, con=con, schema=postgresSchema, index=False, dtype=col_dtypes, if_exists='append')


def testinsertOEE():
    availability = 0.98
    performance = 0.99
    quality = 0.95
    oee = availability * performance * quality
    throughput = 8
    insertOEE('urn:ngsi_ld:Workstation:1', availability, performance, quality, oee, throughput, 'urn:ngsi_ld:Job:202200045')


def testcalculateOEE1():
    oeeData = calculateOEE('urn:ngsi_ld:Workstation:1', 'urn:ngsi_ld:Job:202200045', _time_override=stringToDateTime('2022-04-05 13:38:27.87'))
    if oeeData is not None:
        (availability, performance, quality, oee, throughput) = oeeData
        logger_OEE.debug(f'Availability: {availability}, Performance: {performance}, Quality: {quality}, OEE: {oee}, Throughput: {throughput}')
        insertOEE('urn:ngsi_ld:Workstation:1', availability, performance, quality, oee, throughput, 'urn:ngsi_ld:Job:202200045')


def testcalculateOEEall():
    firstTimeStamp = 1649047794800
    lastTimeStamp = 1649343921547 + 2*3600*1e3
    timestamp = firstTimeStamp
    jobId = 'urn:ngsi_ld:Job:202200045'
    try:
        while timestamp <= lastTimeStamp:
            logger_OEE.info(f'Calculating OEE data for timestamp: {timestamp}')
            oeeData = calculateOEE('urn:ngsi_ld:Workstation:1', jobId, _time_override=stringToDateTime(msToDateTimeString(timestamp)))
            if oeeData is not None:
                logger_OEE.info(f'OEE data: {oeeData}')
                (availability, performance, quality, oee, throughput) = oeeData
                logger_OEE.debug(f'Availability: {availability}, Performance: {performance}, Quality: {quality}, OEE: {oee}, Throughput: {throughput}')
                insertOEE('urn:ngsi_ld:Workstation:1', availability, performance, quality, oee, throughput, jobId, _time_override=stringToDateTime(msToDateTimeString(timestamp)))
            timestamp += 60e3
    except KeyboardInterrupt:
        logger_OEE.info('KeyboardInterrupt. Exiting.')
        con.close()
        engine.dispose()
        sys.exit(0)


if __name__ == '__main__':
    global engine, con
    engine = create_engine(f'postgresql://{conf["postgresUser"]}:{conf["postgresPassword"]}@{conf["postgresHost"]}:5432')
    con = engine.connect()
    testcalculateOEE1()
    testinsertOEE()
    # testcalculateOEEall()
    con.close()
    engine.dispose()
