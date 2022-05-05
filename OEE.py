# -*- coding: utf-8 -*-
# Standard Library imports
from datetime import datetime
import time

# PyPI packages
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.schema import CreateSchema
from sqlalchemy.types import Text

# custom imports
from conf import conf
import Orion

global engine, con
global conf, postgresSchema, day
postgresSchema = conf['postgresSchema']

DATETIME_FORMAT='%Y-%m-%d %H:%M:%S.%f'

def msToDateTimeString(ms):
    return str(datetime.fromtimestamp(ms/1000.0).strftime(DATETIME_FORMAT))[:-3]

def stringToDateTime(string):
    return datetime.strptime(string, DATETIME_FORMAT)

def calculateOEE(day, workstationId, jobId, _time_override=False):
    # todo: implement _time_override behaviour for testing purposes
    # _time_override will never be used in production,we will work with the current time
    now = datetime.now()
    if _time_override:
        now = _time_override

    #integer unix time in milliseconds at 0:00:00
    timeTodayStart = int(day.timestamp()*1000)
    #integer unix time in milliseconds at 24:00:00
    timeTodayEnd = int((day + pd.DateOffset(days=1)).timestamp()*1000)
    
    # availability        
    # todo: use workstationId 
    df = pd.read_sql_query(f'select * from default_service.urn_ngsi_ld_workstation_1_workstation',con=con)
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
    # todo: use Orion.getObject to fetch OperatorSchedule object as JSON, extract these variables
    OperatorScheduleStartsAt = stringToDateTime('2022-04-10 8:00:00.000')
    OperatorScheduleStopsAt = stringToDateTime('2022-04-10 16:00:00.000')
    
    # todo: we do not change OperatorScheduleStopsAt, use a variable called "now" instead
    if (df.iloc[-1].attrvalue =='true'):
        OperatorScheduleStopsAt = datetime.now()
    
    availability = total_available_time/(OperatorScheduleStopsAt.timestamp()*1000-OperatorScheduleStartsAt.timestamp()*1000)
    
    # performance, quality
    # todo use jobId
    df = pd.read_sql_query(f'select * from default_service.urn_ngsi_ld_job_202200045_job',con=con)
    # convert timestamps to integers
    df['recvtimets'] = df['recvtimets'].map(float)
    df['recvtimets'] = df['recvtimets'].map(int)
    # filter for today
    df = df[(timeTodayStart < df.recvtimets) & (df.recvtimets <= timeTodayEnd)]
    
    if df.size == 0:
        quality = 0
        performance = 0
    else:
        GoodPartCounter = int(df[df.attrname == 'GoodPartCounter'].iloc[-1].attrvalue)
        RejectPartCounter = int(df[df.attrname == 'RejectPartCounter'].iloc[-1].attrvalue)
        TotalProductCounter = GoodPartCounter + RejectPartCounter
    
        quality = (TotalProductCounter - RejectPartCounter) / TotalProductCounter
        
        StartGoodPartCounter = int(df[df.attrname == 'GoodPartCounter'].iloc[0].attrvalue)
        CurrentGoodPartCounter = int(df[df.attrname == 'GoodPartCounter'].iloc[-1].attrvalue)
        TodayGoodPartCounter = CurrentGoodPartCounter - StartGoodPartCounter
        
        # we already calculated the total_available_time for today, we can use it here
        StartTime = int(df[df.attrname == 'GoodPartCounter'].iloc[0].recvtimets)
        CurrentTime = int(df[df.attrname == 'GoodPartCounter'].iloc[-1].recvtimets)
        FullTime = CurrentTime - StartTime
        
        ReferenceJobTime = 6000
        
        performance = ReferenceJobTime * TodayGoodPartCounter / FullTime
 
        
    
    """
    # we do not need to know the exact number of good parts or reject parts made today
    # find n_successful_mouldings, n_failed_mouldings
    # there may be duplicate rows that do not represent a moulding, that is why we use unique
    n_successful_mouldings = len(df[df['attrname'] == 'GoodPartCounter']['GoodPartCounter'].unique())
    n_failed_mouldings = len(df[df['attrname'] == 'RejectPartCounter']['RejectPartCounter'].unique())
    n_total_mouldings = n_successful_mouldings + n_failed_mouldings

    # todo get Job from Orion based on jobId
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
    # todo insert a new row into the OEE table
    # create table if not exists, append row if it does
    pass

def testcalculateOEE():
    day = datetime(2022, 4, 8)
    global engine, con
    engine = create_engine('postgresql://{}:{}@localhost:5432'.format(conf['postgresUser'], conf['postgresPassword']))
    con = engine.connect()
    # some work needs to be done on Andor's side
    #####availability, performance, quality, oee = calculateOEE(day, 'urn:ngsi_ld:Job:202200045', 'urn:ngsi_ld:Workstation:1')
    #print(f'availability: {availability}, performance: {performance}, quality: {quality}, oee: {oee}\n')
    # todo why does it return only 2 values? I see 4.
    [availability, quality] = calculateOEE(day, 'urn:ngsi_ld:Job:202200045', 'urn:ngsi_ld:Workstation:1')
    print('Availability: ', availability, '\nQuality: ', quality)
    con.close()
    engine.dispose()

if __name__ == '__main__':
    testcalculateOEE()

