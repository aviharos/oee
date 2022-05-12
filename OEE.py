# -*- coding: utf-8 -*-
# Standard Library imports
from datetime import datetime
import time

# PyPI packages
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.schema import CreateSchema
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

def calculateOEE(day, workstationId, jobId, _time_override=False):
    # todo: implement _time_override behaviour for testing purposes :)
    # _time_override will never be used in production,we will work with the current time
    now = datetime.now()
    if _time_override:
        now = _time_override

    #integer unix time in milliseconds at 0:00:00
    timeTodayStart = int(day.timestamp()*1000)
    #integer unix time in milliseconds at 24:00:00
    if _time_override:
        timeTodayEnd = now.timestamp()*1000
    else:
        timeTodayEnd = int((day + pd.DateOffset(days=1)).timestamp()*1000)
    print(timeTodayEnd)
    
    # availability        
    # todo: use workstationId :)
    workstation = workstationId.replace(':', '_').replace('W', 'w') + '_workstation'
    df = pd.read_sql_query(f'select * from default_service.' + workstation,con=con)
    # convert timestamps to integers
    df['recvtimets'] = df['recvtimets'].map(float)
    df['recvtimets'] = df['recvtimets'].map(int)
    # filter data for today
    df = df[(timeTodayStart < df.recvtimets) & (df.recvtimets <= timeTodayEnd)]
    # Available is true and false in this periodical order, starting with true
    # we can sum the timestamps of the true values and the false values disctinctly, getting 2 sums
    # the total available time is their difference    
    available_true = df[df.attrvalue == 'true'] #filter_ data # pandas DataFrame
    available_false = df[df.attrvalue == 'false'] #filter_ data # pandas DataFrame
    total_available_time = available_false['recvtimets'].sum() - available_true['recvtimets'].sum()
    
    # if the Workstation is available currently, we need to add the current timestamp to the true timestamps' sum
    if (df.iloc[-1].attrvalue =='true'):
        now_unix = now.timestamp()
        total_available_time += now_unix*1000
        
    #total_available_time = datetime.utcfromtimestamp(total_available_time).strftime(DATETIME_FORMAT)
    total_available_time_hours = time.strftime("%H:%M:%S", time.gmtime(total_available_time/1000))
    
    print('Total available time: ', total_available_time_hours)
    
    # OperatorScheduleStartsAt and the OperatorScheduleStopsAt will be requested from the Orion broker
    # if the shift has not ended yet, the current time needs to be used instead of OperatorScheduleStopsAt
    # todo: use Orion.getObject to fetch OperatorSchedule object as JSON, extract these variables :)
    
    sch_json = str(Orion.getObject('urn:ngsi_ld:OperatorSchedule:1'))
    
    find1 = sch_json.find('OperatorWorkingScheduleStartsAt')
    OperatorScheduleStartsAt = datetime.strptime('2022-05-01 ' + str(sch_json[find1+50:find1+80][11:sch_json[find1+50:find1+80].find(',')-1]), '%Y-%m-%d %H:%M:%S')
    
    find2 = sch_json.find('OperatorWorkingScheduleStopsAt')
    OperatorScheduleStopsAt = datetime.strptime('2022-05-01 ' + str(sch_json[find2+50:find2+80][10:sch_json[find2+50:find2+80].find(',')-1]), '%Y-%m-%d %H:%M:%S')
    
    # todo return None if now is past the OperatorScheduleStopsAt value :(
    
    # todo: we do not change OperatorScheduleStopsAt, use a variable called "now" instead :)
    if (df.iloc[-1].attrvalue =='true'):
        availability = total_available_time/(now.timestamp()*1000-OperatorScheduleStartsAt.timestamp()*1000)
    else:
        availability = total_available_time/(OperatorScheduleStopsAt.timestamp()*1000-OperatorScheduleStartsAt.timestamp()*1000)
    
    # performance, quality
    # todo use jobId :)
    job = jobId.replace(':', '_').replace('J', 'j') + '_job'
    df = pd.read_sql_query(f'select * from default_service.' + job,con=con)
    # convert timestamps to integers
    df['recvtimets'] = df['recvtimets'].map(float)
    df['recvtimets'] = df['recvtimets'].map(int)
    # filter for today
    df = df[(timeTodayStart < df.recvtimets) & (df.recvtimets <= timeTodayEnd)]
    
    if df.size == 0:
        #return None !!!!!!!!!!
        quality = 0
        performance = 0
    else:
        n_successful_mouldings = len(df[df.attrname == 'GoodPartCounter']['attrvalue'].unique())
        n_failed_mouldings = len(df[df.attrname == 'RejectPartCounter']['attrvalue'].unique())
        n_total_mouldings = n_successful_mouldings + n_failed_mouldings
        
        job_json = str(Orion.getObject(jobId))
        find1 = job_json[job_json.find('JobType'):job_json.find('JobType')+60]
        job_type = find1[40:find1.find('}')-14]
        print(job_type)
        
        # a jobtype itt 'JobCover', ez alapján kellene megtalálni a 'TOOL_COVER_REFERENCE_INJECTION_TIME'-t?
        
        constants_json = str(Orion.getObject('urn:ngsi_ld:Constants:1'))
        
        tofind = job_type.upper() + '_REFERENCE_INJECTION_TIME'
        
        find1 = constants_json[constants_json.find(tofind):constants_json.find(tofind)+80]
        
        referenceInjectionTime = float(find1[find1.find('value') + 8:find1.find('metadata')-3]) * 1000
        
        performance = n_total_mouldings * referenceInjectionTime / total_available_time
        quality = n_successful_mouldings / n_total_mouldings
        
        
    oee = availability * performance * quality
    
    return availability, performance, quality, oee

def insertOEE(workstationId, availability, performance, quality, oee):
    # todo insert a new row into the OEE table :) 
    # create table if not exists, append row if it does
    table_name = workstationId.replace(':', '_').replace('W', 'w') + '_workstation_oee_tryyyyyy'
    current_time = datetime.now().timestamp()*1000

    #csak akkor fogadta el a timestamp számokat, ha az oszlopok nevét átírtam valami értelmetlenre az eredetiről, mert így már nem vár idő formátumot    
    df = pd.DataFrame(data=[[current_time,current_time - 7200000, availability, performance, quality, oee, 4, 'job']], columns=['xrecvtimets','xrecvtime','availability','performance','quality','oee','throughput_shift','jobs'])

    df.iloc[[0]].to_sql(name=table_name, con=con, schema=postgresSchema, index=False, dtype=col_dtypes, if_exists='append')
    
    df_read = pd.read_sql_query(f'select * from default_service.' + table_name,con=con)
    print(df_read)
    
    
    pass

def testcalculateOEE():
    day = datetime(2022, 4, 4)
    global engine, con
    engine = create_engine('postgresql://{}:{}@localhost:5432'.format(conf['postgresUser'], conf['postgresPassword']))
    con = engine.connect()
    # some work needs to be done on Andor's side
    #print(f'availability: {availability}, performance: {performance}, quality: {quality}, oee: {oee}\n')
    # todo why does it return only 2 values? I see 4. :)
    [availability, performance, quality, oee] = calculateOEE(day, 'urn:ngsi_ld:Workstation:1', 'urn:ngsi_ld:Job:202200045', stringToDateTime('2022-04-04 16:38:27.87'))
    print('Availability: ', availability, '\nPerformance: ', performance, '\nQuality: ', quality, '\nOEE: ', oee)
    
    insertOEE('urn:ngsi_ld:Workstation:1', availability, performance, quality, oee)
    
    con.close()
    engine.dispose()

if __name__ == '__main__':
    testcalculateOEE()

