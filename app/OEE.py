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


class OEE():
    def __init__(self, wsId):
        self.operatorSchedule = None
        self.wsId = None
        self.ws = None
        self.job = None
        self.part = None
        self.ws_df = None
        self.job_df = None
        self.logger = getLogger(__name__)
        self.DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
        self.col_dtypes = {'recvtimets': BigInteger(),
                           'recvtime': DateTime(),
                           'availability': Float(),
                           'performance': Float(),
                           'quality': Float(),
                           'oee': Float(),
                           'throughput_shift': Float(),
                           'jobs': Text()}

    def msToDateTimeString(self, ms):
        return str(datetime.fromtimestamp(ms/1000.0).strftime(self.DATETIME_FORMAT))[:-3]

    def stringToDateTime(self, string):
        return datetime.strptime(string, self.DATETIME_FORMAT)
    
    def timeToDatetime(self, string):
        datetime.strptime(str(now.date() + ' ' + string), '%Y-%m-%d %H:%M:%S')

    def find_operation(self, part, operationType):
        for operation in part['Operations']['value']:
            if operation['OperationType']['value'] == operationType:
                return operation
        raise AttributeError(f'The part {part} has no operation with type {operationType}')

    def updateObjects(self):
        self.operatorSchedule = Orion.getObject('urn:ngsi_ld:OperatorSchedule:1')
        try:
            for time_ in ('OperatorScheduleStartsAt', 'OperatorScheduleStopsAt'):
                self.today[time_] = self.timeToDatetime(self.operatorSchedule[time_]['value'])
        except (ValueError, KeyError) as error:
            raise ValueError(f'Critical: could not convert time in Operatorschedule. Traceback:\n{error}')
        self.ws = Orion.getObject(self.wsId)
        del(self.wsId)
        self.job = Orion.getObject(self.ws['RefJob']['value'])
        self.part = Orion.getObject(self.job['RefPart']['value'])

    def checkTime(self):
        if self.now < self.today['OperatorScheduleStartsAt']:
            self.logger.info(f'The shift has not started yet before time: {self.now}, no OEE data')
            return False
        if self.now > self.today['OperatorScheduleStopsAt']:
            self.logger.info(f'The current time: {self.now} is after the shift\'s end, no OEE data')
            return False

    def areConditionsOK(self):
        if (not self.checkTime(),
            not True):
            return False

    def prepare(self):
        self.now = datetime.now()
        self.now_unix = datetime.now().timestamp() * 1000
        self.today = {'day': datetime.now().date()}
        try:
            self.updateObjects()
        except (RuntimeError, KeyError, AttributeError) as error:
            self.logger.error(f'Could not prepare for OEE calculations. Traceback:\n{error}')
            raise error
        if not self.areConditionsOK():
            self.logger

    def calculateOEE(self, workstationId, jobId, con,):

        # availability
        workstation = workstationId.replace(':', '_').lower() + '_workstation'
        try:
            df = pd.read_sql_query(f'''select * from {conf['postgresSchema']}.{workstation}
                                   where {timeTodayStart} < cast (recvtimets as bigint)
                                   and cast (recvtimets as bigint) <= {now_unix};''', con=con)
        except (psycopg2.errors.UndefinedTable,
                sqlalchemy.exc.ProgrammingError) as error:
            self.logger.error(f'The SQL table: {workstation} does not exist within the schema: {conf["postgresSchema"]}. Traceback:\n{error}')
            return None

        df['recvtimets'] = df['recvtimets'].map(float).map(int)

        if df.size == 0:
            self.logger.warning(f'No workstation data found for {workstationId} up to time {now} on day {today}, no OEE data')
            return None

        # Available is true and false in this periodical order, starting with true
        # we can sum the timestamps of the true values and the false values disctinctly, getting 2 sums
        # the total available time is their difference
        df_av = df[df['attrname'] == 'Available']
        available_true = df_av[df_av['attrvalue'] == 'true']
        available_false = df_av[df_av['attrvalue'] == 'false']
        total_available_time = available_false['recvtimets'].sum() - available_true['recvtimets'].sum()
        # if the Workstation is available currently, we need to add
        # the current timestamp to the true timestamps' sum
        if (df_av.iloc[-1]['attrvalue'] == 'true'):
            total_available_time += now_unix

        total_available_time_hours = time.strftime("%H:%M:%S", time.gmtime(total_available_time/1000))

        availability = total_available_time/(now.timestamp()*1000-OperatorScheduleStartsAt.timestamp()*1000)

        # performance, quality
        job = jobId.replace(':', '_').lower() + '_job'
        try:
            df = pd.read_sql_query(f'''select * from {conf['postgresSchema']}.{job}
                                   where {timeTodayStart} < cast (recvtimets as bigint)
                                   and cast (recvtimets as bigint) <= {now_unix};''', con=con)
        except (psycopg2.errors.UndefinedTable,
                sqlalchemy.exc.ProgrammingError) as error:
            self.logger.error(f'The SQL table: {job} does not exist within the schema: {conf["postgresSchema"]}. Traceback:\n{error}')
            return None
        df['recvtimets'] = df['recvtimets'].map(float).map(int)

        if df.size == 0:
            self.logger.warning(f'No job data found for {jobId} up to time {now} on day {today}.')
            return None

        n_successful_mouldings = len(df[df.attrname == 'GoodPartCounter']['attrvalue'].unique())
        n_failed_mouldings = len(df[df.attrname == 'RejectPartCounter']['attrvalue'].unique())
        n_total_mouldings = n_successful_mouldings + n_failed_mouldings

        if n_total_mouldings == 0:
            self.logger.warning('No operation was completed yet, no OEE data')
            return None

        status_code, job_json = Orion.getObject(jobId)
        if status_code != 200:
            self.logger.error(f'Failed to get object from Orion broker:{jobId}, status_code:{status_code}; no OEE data')
            return None

        try:
            partId = job_json['RefPart']['value']
        except (KeyError, TypeError):
            self.logger.critical('Critical: RefPart not found in the Job {jobId}: {job_json}')
            return None

        status_code, part_json = Orion.getObject(partId)
        if status_code != 200:
            self.logger.error(f'Failed to get object from Orion broker:{partId}, status_code:{status_code}; no OEE data')
            return None

        try:
            current_operation_type = job_json['CurrentOperationType']['value']
        except (KeyError, TypeError):
            self.logger.critical(f'Critical: CurrentOperationType not found in the Job {jobId}: {job_json}')
            return None

        try:
            operation = self.find_operation(part_json, current_operation_type)
        except (KeyError, TypeError):
            self.logger.critical(f'Critical: Operation {current_operation_type} not found in the Part: {part_json}')
            return None

        try:
            operationTime = operation['OperationTime']['value']
        except (KeyError, TypeError):
            self.logger.critical(f'Critical: OperationTime not found in the Part: {part_json}')
            return None

        try:
            partsPerOperation = operation['PartsPerOperation']['value']
        except (KeyError, TypeError):
            self.logger.critical(f'Critical: partsPerOperation not found in the Part: {part_json}')
            return None

        performance = n_total_mouldings * operationTime / total_available_time
        quality = n_successful_mouldings / n_total_mouldings
        oee = availability * performance * quality
        shiftLengthInMilliseconds = OperatorScheduleStopsAt.timestamp()*1000 - OperatorScheduleStartsAt.timestamp()*1000
        throughput = (shiftLengthInMilliseconds / operationTime) * partsPerOperation * oee
        self.logger.info(f'Availability: {availability}, Performance: {performance}, Quality: {quality}, OEE: {oee}, Throughput: {throughput}')

        return availability, performance, quality, oee, throughput

    def insert(self, workstationId, availability, performance, quality, oee, throughput, jobIds, con, _time_override=None):
        table_name = workstationId.replace(':', '_').lower() + '_workstation_oee'
        now = datetime.now()
        if _time_override is not None:
            now = _time_override
            self.logger.warning(f'Time override in insertOEE: {_time_override}')
        now_unix = now.timestamp()*1000
        oeeData = pd.DataFrame.from_dict({'recvtimets': [now_unix],
                                          'recvtime': [self.msToDateTimeString(now_unix)],
                                          'availability': [availability],
                                          'performance': [performance],
                                          'quality': [quality],
                                          'oee': [oee],
                                          'throughput_shift': [throughput],
                                          'jobs': [jobIds]})
        oeeData.to_sql(name=table_name, con=con, schema=conf['postgresSchema'], index=False, dtype=self.col_dtypes, if_exists='append')
        self.logger.debug('Successfully inserted OEE data into Postgres')


def testinsertOEE(con):
    availability = 0.98
    performance = 0.99
    quality = 0.95
    oee = availability * performance * quality
    throughput = 8
    insertOEE('urn:ngsi_ld:Workstation:1', availability, performance, quality, oee, throughput, 'urn:ngsi_ld:Job:202200045', con)


def testcalculateOEE1(con):
    oeeData = calculateOEE('urn:ngsi_ld:Workstation:1', 'urn:ngsi_ld:Job:202200045', con, _time_override=stringToDateTime('2022-04-05 13:38:27.87'))
    if oeeData is not None:
        (availability, performance, quality, oee, throughput) = oeeData
        insertOEE('urn:ngsi_ld:Workstation:1', availability, performance, quality, oee, throughput, 'urn:ngsi_ld:Job:202200045', con)


def testcalculateOEEall(con):
    firstTimeStamp = 1649047794800
    lastTimeStamp = 1649343921547 + 2*3600*1e3
    timestamp = firstTimeStamp
    jobId = 'urn:ngsi_ld:Job:202200045'
    try:
        while timestamp <= lastTimeStamp:
            logger_OEE.info(f'Calculating OEE data for timestamp: {timestamp}')
            oeeData = calculateOEE('urn:ngsi_ld:Workstation:1', jobId, con, _time_override=stringToDateTime(msToDateTimeString(timestamp)))
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
    engine = create_engine(f'postgresql://{conf["postgresUser"]}:{conf["postgresPassword"]}@{conf["postgresHost"]}:5432')
    con = engine.connect()
    testcalculateOEE1(con=con)
    testinsertOEE(con=con)
    # testcalculateOEEall(con=con)
    con.close()
    engine.dispose()
