# -*- coding: utf-8 -*-
# Standard Library imports
from datetime import datetime
import glob
import time
import os

# PyPI packages
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.schema import CreateSchema
from sqlalchemy.types import Text

# custom imports
from conf import conf
# from Orion import *

global engine, con
global conf, postgresSchema, day
postgresSchema = conf['postgresSchema']

DATETIME_FORMAT='%Y-%m-%d %H:%M:%S.%f'

def msToDateTimeString(ms):
    return str(datetime.fromtimestamp(ms/1000.0).strftime(DATETIME_FORMAT))[:-3]

def stringToDateTime(string):
    return datetime.strptime(string, DATETIME_FORMAT)

def calculateOEE(day, workstationId, jobId):
    # this is how you can read a value from a DataFrame
    # value = df['column'].iloc[number of row]

    timeTodayStart = int(day.timestamp()*1000) #integer unix time in milliseconds at 0:00:00
    timeTodayEnd = int((day + pd.DateOffset(days=1)).timestamp()*1000) #integer unix time in milliseconds at 24:00:00
    
    # availability
    # we read the Workstation logs into a pandas DataFrame (that is in memory)

    
    global engine, con
    engine = create_engine('postgresql://{}:{}@localhost:5432'.format(conf['postgresUser'], conf['postgresPassword']))
    con = engine.connect()
    
    #workstationIds = getWorkstationIdsFromOrion()
    
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema=postgresSchema)

    
    df = pd.read_sql_query(f'select * from default_service.urn_ngsi_ld_workstation_1_workstation',con=con)
    # convert timestamps to integers
    df['recvtimets'] = df['recvtimets'].map(float)
    #df['recvtimets'] = dpd.to_numeric(df['recvtimets'], downcast="float")
    df['recvtimets'] = df['recvtimets'].map(int)
    df['recvtimets'] = df['recvtimets'] - 7200
    # filter data for today
    df = df[(timeTodayStart < df.recvtimets) & (df.recvtimets <= timeTodayEnd)]
    # Available is true and false in this periodical order, starting with true
    # we can sum the timestamps of the true values and the false values disctinctly, getting 2 sums
    # the total available time is their difference
    # if the Workstation is available currently, we need to add the current timestamp to the true timestamps' sum
    
    available_true = df[df.attrvalue == 'true'] #filter_ data # pandas DataFrame
    available_false = df[df.attrvalue == 'false'] #filter_ data # pandas DataFrame
    total_available_time = available_false['recvtimets'].sum() - available_true['recvtimets'].sum()
    
    if (df.iloc[-1].attrvalue =='true'):
        #current = '2022-04-11 15:52:00.000'
        #current_datetime = stringToDateTime(str(current))
        current_datetime = datetime.now()
        current_unix = current_datetime.timestamp()
        total_available_time += current_datetime.timestamp()*1000
        
        
    
        
    #total_available_time = datetime.utcfromtimestamp(total_available_time).strftime(DATETIME_FORMAT)
    total_available_time_hours = time.strftime("%H:%M:%S", time.gmtime(total_available_time/1000))
    
    print('Total available time: ', total_available_time_hours)
    
    # OperatorScheduleStartsAt and the OperatorScheduleStopsAt will be global variables, available for use
    # if the shift has not ended yet, the current time needs to be used instead of OperatorScheduleStopsAt
    """
    df_OperatorSchedule = pd.read_sql_query(f'select * from default_service.urn_ngsi_ld_operatorschedule_1_operatorschedule',con=con)
    
    OperatorScheduleStartsAt = df_OperatorSchedule[df_OperatorSchedule.attrname == 'OperatorWorkingScheduleStartsAt']
    OperatorScheduleStopsAt = df_OperatorSchedule[df_OperatorSchedule.attrname == 'OperatorWorkingScheduleStopsAt']
    
    OperatorScheduleStartsAt = str(OperatorScheduleStartsAt.attrvalue)[5:12]
    OperatorScheduleStopsAt = str(OperatorScheduleStopsAt.attrvalue)[5:12]
    OperatorScheduleStartsAt = datetime.strptime(OperatorScheduleStartsAt, '%H:%M:%S')
    OperatorScheduleStopsAt = datetime.strptime(OperatorScheduleStopsAt, '%H:%M:%S')
    
    #OperatorScheduleStartsAt = OperatorScheduleStartsAt.timestamp()
    #OperatorScheduleStopsAt = datetime.strptime(OperatorScheduleStopsAt, '%H:%M:%S')
    """
    OperatorScheduleStartsAt = stringToDateTime('2022-04-10 8:00:00.000')
    OperatorScheduleStopsAt = stringToDateTime('2022-04-10 16:00:00.000')
    

    
    if (df.iloc[-1].attrvalue =='true'):
        OperatorScheduleStopsAt = datetime.now()
    
    availability = total_available_time/(OperatorScheduleStopsAt.timestamp()*1000-OperatorScheduleStartsAt.timestamp()*1000)
    

    
    
    # performance, quality
    # Job log table name from job ID, replace old object df
    df = pd.read_sql_query(f'select * from default_service.urn_ngsi_ld_job_202200045_job',con=con)
    # convert timestamps to integers
    df['recvtimets'] = df['recvtimets'].map(float)
    df['recvtimets'] = df['recvtimets'].map(int)
    df['recvtimets'] = df['recvtimets'] - 7200
    # filter for today
    df = df[(timeTodayStart < df.recvtimets) & (df.recvtimets <= timeTodayEnd)]
    # we can make vectors of Booleans if checking a column of a DataFrame against a string, then we sum the vectors, thus counting the True-s
    
    
    if df.size == 0:
        quality = 0
        
    else:
        GoodPartCounter = int(df[df.attrname == 'GoodPartCounter'].iloc[-1].attrvalue)
        RejectPartCounter = int(df[df.attrname == 'RejectPartCounter'].iloc[-1].attrvalue)
        TotalProductCounter = GoodPartCounter + RejectPartCounter
    
        quality = (TotalProductCounter - RejectPartCounter) / TotalProductCounter
    
    """
    
    n_successful_mouldings = [df['attrname'] == 'GoodPartCounter']].sum()
    n_failed_mouldings = [df['attrname'] == 'RejectPartCounter']].sum()
    n_total_mouldings = n_successful_mouldings + n_failed_mouldings

    # referenceInjectionTime depends on the JobId - look up
    performance = n_total_mouldings * referenceInjectionTime / total_available_time

    quality = n_successful_mouldings / n_total_mouldings

    oee = availability * performance * quality
    return availability, performance, quality, oee
    or if the oee cannot be calculated for some reason
    
    return None
"""

    return availability, quality

def updateOEE(workstationId, availability, performance, quality, oee):
    pass

def test():
    day = datetime(2022, 4, 8)
    # some work needs to be done on Andor's side
    #####availability, performance, quality, oee = calculateOEE(day, 'urn:ngsi_ld:Job:202200045', 'urn:ngsi_ld:Workstation:1')
    #print(f'availability: {availability}, performance: {performance}, quality: {quality}, oee: {oee}\n')
    [availability, quality] = calculateOEE(day, 'urn:ngsi_ld:Job:202200045', 'urn:ngsi_ld:Workstation:1')
    print('Availability: ', availability, '\nQuality: ', quality)


if __name__ == '__main__':
    test()

