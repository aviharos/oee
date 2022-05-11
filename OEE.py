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
    # todo: use Orion.getObject to fetch OperatorSchedule object as JSON, extract these variables :(
    
    JSON = Orion.getObject('urn:ngsi_ld:OperatorSchedule:1')
    #print(JSON)
    OperatorScheduleStartsAt = stringToDateTime('2022-04-10 8:00:00.000')
    OperatorScheduleStopsAt = stringToDateTime('2022-04-10 16:00:00.000')
    
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
        
        referenceInjectionTime = 50000
        performance = n_total_mouldings * referenceInjectionTime / total_available_time
        quality = n_successful_mouldings / n_total_mouldings
        
        
    
    """
    # we do not need to know the exact number of good parts or reject parts made today
    # find n_successful_mouldings, n_failed_mouldings
    # there may be duplicate rows that do not represent a moulding, that is why we use unique
    # 4000, 4000, 4000, 4008, 4008, 4008, 4016, 4016, 4024
    # [4000, 4008, 4016, 4024]
    n_successful_mouldings = len(df[df['attrname'] == 'GoodPartCounter']['GoodPartCounter'].unique())
    n_failed_mouldings = len(df[df['attrname'] == 'RejectPartCounter']['RejectPartCounter'].unique())
    n_total_mouldings = n_successful_mouldings + n_failed_mouldings

    # todo get Job from Orion based on jobId :(
    # todo extract JobTpye from JSON
    # todo get urn:ngsi_ld:Constants:1 from Orion, extract referenceInjectionTime
    # referenceInjectionTime depends on the JobId - look up
    # also, check that the code works even if there is no counter at all for one of the types, for example no reject
    # it should work because of len(unique)
    performance = n_total_mouldings * referenceInjectionTime / total_available_time

    quality = n_successful_mouldings / n_total_mouldings

    """
    oee = availability * performance * quality
    
    return availability, performance, quality, oee

def insertOEE(workstationId, availability, performance, quality, oee):
    # todo insert a new row into the OEE table :) 
    # create table if not exists, append row if it does
    table_name = workstationId.replace(':', '_').replace('W', 'w') + '_workstation_oee_tryyyyyy'
    current_time = datetime.now().timestamp()*1000

    #csak akkor fogadta el a timestamp számokat, ha az oszlopok nevét átírtam valami értelmetlenre az eredetiről, mert így már nem vár idő formátumot    
    df = pd.DataFrame(data=[[current_time,current_time - 7200000, availability, performance, quality, oee,4,'job']], columns=['xrecvtimets','xrecvtime','availability','performance','quality','oee','throughput_shift','jobs'])

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
    #####availability, performance, quality, oee = calculateOEE(day, 'urn:ngsi_ld:Job:202200045', 'urn:ngsi_ld:Workstation:1')
    #print(f'availability: {availability}, performance: {performance}, quality: {quality}, oee: {oee}\n')
    # todo why does it return only 2 values? I see 4. :)
    [availability, performance, quality, oee] = calculateOEE(day, 'urn:ngsi_ld:Workstation:1', 'urn:ngsi_ld:Job:202200045', stringToDateTime('2022-04-04 16:38:27.87'))
    print('Availability: ', availability, '\nPerformance: ', performance, '\nQuality: ', quality, '\nOEE: ', oee)
    
    insertOEE('urn:ngsi_ld:Workstation:1', availability, performance, quality, oee)
    
    con.close()
    engine.dispose()

if __name__ == '__main__':
    testcalculateOEE()

