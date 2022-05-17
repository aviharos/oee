# -*- coding: utf-8 -*-
# Standard Library imports
from datetime import datetime
import time

# PyPI packages
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import DateTime, Float, BigInteger, Text

# custom imports
from conf import conf
import Orion

global engine, con
global conf, postgresSchema, day
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

def calculateOEE(workstationId, jobId, _time_override=False):
    # _time_override will never be used in production,we will work with the current time
    now = datetime.now()
    if _time_override:
        now = _time_override
    
    #integer unix time in milliseconds at 00:00:00
    timeTodayStart = int(stringToDateTime(str(now.date()) + ' 00:00:00.000').timestamp()*1000)
    
    now_unix = now.timestamp()*1000

    # availability        
    workstation = workstationId.replace(':', '_').lower() + '_workstation'
    df = pd.read_sql_query(f'select * from default_service.{workstation}',con=con)      
    df['recvtimets'] = df['recvtimets'].map(float)
    df['recvtimets'] = df['recvtimets'].map(int)
    df = df[(timeTodayStart < df.recvtimets) & (df.recvtimets <= now_unix)]

    if df.size == 0:
        return None

    # Available is true and false in this periodical order, starting with true
    # we can sum the timestamps of the true values and the false values disctinctly, getting 2 sums
    # the total available time is their difference    
    available_true = df[df.attrvalue == 'true']
    available_false = df[df.attrvalue == 'false']
    total_available_time = available_false['recvtimets'].sum() - available_true['recvtimets'].sum()
    # if the Workstation is available currently, we need to add
    # the current timestamp to the true timestamps' sum
    if (df.iloc[-1].attrvalue =='true'):
        total_available_time += now_unix
    
    total_available_time_hours = time.strftime("%H:%M:%S", time.gmtime(total_available_time/1000))
    
    print('Total available time: ', total_available_time_hours)
    
    # OperatorScheduleStartsAt and the OperatorScheduleStopsAt
    # will be requested from the Orion broker
    # if the shift has not ended yet, the current time needs to be used
    # instead of OperatorScheduleStopsAt
    
    status_code, sch_json = Orion.getObject('urn:ngsi_ld:OperatorSchedule:1')
    OperatorScheduleStartsAt = datetime.strptime(str(now.date())+ ' ' + str(sch_json['OperatorWorkingScheduleStartsAt']['value']), '%Y-%m-%d %H:%M:%S')        
    OperatorScheduleStopsAt = datetime.strptime(str(now.date())+ ' ' + str(sch_json['OperatorWorkingScheduleStopsAt']['value']), '%Y-%m-%d %H:%M:%S')
    
    if now < OperatorScheduleStartsAt:
        return None
    if now > OperatorScheduleStopsAt:
        return None
    
    availability = total_available_time/(now.timestamp()*1000-OperatorScheduleStartsAt.timestamp()*1000)
    
    # performance, quality
    job = jobId.replace(':', '_').lower() + '_job'
    df = pd.read_sql_query(f'select * from default_service.{job}',con=con)
    # convert timestamps to integers
    df['recvtimets'] = df['recvtimets'].map(float)
    df['recvtimets'] = df['recvtimets'].map(int)
    # filter for today
    df = df[(timeTodayStart < df.recvtimets) & (df.recvtimets <= now_unix)]

    if df.size == 0:
        return None
    
    n_successful_mouldings = len(df[df.attrname == 'GoodPartCounter']['attrvalue'].unique())
    n_failed_mouldings = len(df[df.attrname == 'RejectPartCounter']['attrvalue'].unique())
    n_total_mouldings = n_successful_mouldings + n_failed_mouldings
    

    
    status_code, job_json = Orion.getObject(jobId)
    status_code, constants_json = Orion.getObject('urn:ngsi_ld:Constants:1')
    job_type = job_json['JobType']['value']
    
    if job_type == 'JobCover':
        referenceInjectionTime = constants_json['TOOL_COVER_REFERENCE_INJECTION_TIME']['value'] * 1000
        partsPerMoulding = constants_json['COVER_PARTS_PER_MOULDING']['value']
    elif job_type == 'JobCore':
        referenceInjectionTime = constants_json['TOOL_CORE_REFERENCE_INJECTION_TIME']['value'] * 1000
        partsPerMoulding = constants_json['COVER_PARTS_PER_MOULDING']['value']
    elif job_type == 'JobCube':
        referenceInjectionTime = constants_json['TOOL_CUBE_REFERENCE_INJECTION_TIME']['value'] * 1000
        partsPerMoulding = constants_json['COVER_PARTS_PER_MOULDING']['value']
    

    performance = n_total_mouldings * referenceInjectionTime / total_available_time
    
    quality = n_successful_mouldings / n_total_mouldings
        
    oee = availability * performance * quality
    
    
    shiftLengthInMilliseconds = OperatorScheduleStopsAt.timestamp()*1000 - OperatorScheduleStartsAt.timestamp()*1000
    
    throughput = (shiftLengthInMilliseconds / referenceInjectionTime) * partsPerMoulding * oee
    
    return availability, performance, quality, oee, throughput

def insertOEE(workstationId, availability, performance, quality, oee, throughput, jobIds):
    table_name = workstationId.replace(':', '_').lower() + '_workstation_oee'
    now_ms = datetime.now().timestamp()*1000
    oeeData = pd.DataFrame.from_dict({'recvtimets': [now_ms],
                                      'recvtime': [msToDateTimeString(now_ms)],
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

def testcalculateOEE():
    oeeData = calculateOEE('urn:ngsi_ld:Workstation:1', 'urn:ngsi_ld:Job:202200045', _time_override=stringToDateTime('2022-04-05 13:38:27.87'))
    if oeeData is not None:
        (availability, performance, quality, oee, throughput)= oeeData
        print('Availability: ', availability, '\nPerformance: ', performance, '\nQuality: ', quality, '\nOEE: ', oee, '\nThroughput: ', throughput)
        insertOEE('urn:ngsi_ld:Workstation:1', availability, performance, quality, oee, throughput, 'urn:ngsi_ld:Job:202200045')

if __name__ == '__main__':
    global engine, con
    engine = create_engine('postgresql://{}:{}@localhost:5432'.format(conf['postgresUser'], conf['postgresPassword']))
    con = engine.connect()
    testcalculateOEE()
    # testinsertOEE()
    con.close()
    engine.dispose()
