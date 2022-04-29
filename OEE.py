# -*- coding: utf-8 -*-
# Standard Library imports
from datetime import datetime
import os

# PyPI packages
import pandas as pd

DATETIME_FORMAT='%Y-%m-%d %H:%M:%S.%f'

def msToDateTimeString(ms):
    return str(datetime.fromtimestamp(ms/1000.0).strftime(DATETIME_FORMAT))[:-3]

def stringToDateTime(string):
    return datetime.strptime(string, DATETIME_FORMAT)

def calculateOEE(day, workstationId, jobId):
    # this is how you can read a value from a DataFrame
    # value = df['column'].iloc[number of row]

    timeTodayStart = integer unix time in milliseconds at 0:00:00
    timeTodayEnd = integer unix time in milliseconds at 24:00:00
    # availability
    # we read the Workstation logs into a pandas DataFrame (that is in memory)
    df = pd.read_sql_query(...)
    # convert timestamps to integers
    df['recvtimets'] = df['recvtimets'].map(int)
    # filter data for today
    df = df[timeTodayStart < df['recvtimets'] && df['recvtimets'] =< timeTodayEnd]
    # Available is true and false in this periodical order, starting with true
    # we can sum the timestamps of the true values and the false values disctinctly, getting 2 sums
    # the total available time is their difference
    # if the Workstation is available currently, we need to add the current timestamp to the true timestamps' sum
    available_true = filter_ data # pandas DataFrame
    available_false = filter_ data # pandas DataFrame
    total_available_time = available_false['recvtimets'].sum() - available_true['recvtimets'].sum()

    # OperatorScheduleStartsAt and the OperatorScheduleStopsAt will be global variables, available for use
    # if the shift has not ended yet, the current time needs to be used instead of OperatorScheduleStopsAt
    OperatorScheduleStartsAt = 8:00:00
    availability = ...

    # performance, quality
    # Job log table name from job ID, replace old object df
    df = pd.read_sql_query(...)
    # filter for today
    df =
    # we can make vectors of Booleans if checking a column of a DataFrame against a string, then we sum the vectors, thus counting the True-s
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

def updateOEE(workstationId, availability, performance, quality, oee):
    pass

def test:
    day = datetime(2022, 4, 4)
    # some work needs to be done on Andor's side
    availability, performance, quality, oee = calculateOEE(day, 'urn:ngsi_ld:Job:202200045')
    print(f'availability: {availability}, performance: {performance}, quality: {quality}, oee: {oee}\n')

if __name__ == '__main__':
    test()

