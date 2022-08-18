# -*- coding: utf-8 -*-
# Standard Library imports
from datetime import datetime

# PyPI packages
import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.types import DateTime, Float, BigInteger, Text

# custom imports
global conf
from conf import conf
from Logger import getLogger
import Orion


class OEECalculator():
    '''
    An OEE calculator class that builds on Fiware Cygnus logs.
    It uses milliseconds for the time unit, just like Cygnus.

    Purpose:
        Calculating OEE and throughput data

    Disclaimer:
        The OEECalculator class does not consider multiple jobs per shift.
        If a shift contains multiple jobs, the calculations will
        be done as if the shift started when the last job started.

    Usage:
        Configure your Orion JSONS files as in the json directory.
        The Workstation refers to the Job,
            the OEE object
            the Throughput object
        The Job refers to the Part
            and the Operation inside the Part.
        There is also an OperatorSchedule object.
    
    Inputs:
        workstation_id: the Orion id of the Workstation object,
        operatorSchedule_id: the Orion id of the OperatorSchedule object.

    Methods:
        __init__():
            oee = OEECalculator(workstation_id)

        prepare(con):
            Inputs:
                con:
                    the sqlalchemy module's engine's connection object
            downloads data from the Orion broker
                Cygnus logs
            configures itself for today's shift
            if the object cannot prepare, it clears
            all attributes of the OEE object
                Possible reasons:
                    invalid Orion objects (see JSONS files)
                    conectivity issues
                    the current time does not fall within the shift

        calculate_OEE():
            output:
                the Orion OEE object (see sample in JSONS)

        calculate_throughput():
            output:
                the Orion Throughput object (see sample in JSONS)
    '''

    DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
    col_dtypes = {'recvtimets': BigInteger(),
                       'recvtime': DateTime(),
                       'availability': Float(),
                       'performance': Float(),
                       'quality': Float(),
                       'oee': Float(),
                       'throughput_shift': Float(),
                       'job': Text()}
    object_ = {'id': None,
               'orion': None,
               'postgres_table': None,
               'df': None}
    OEE_template =        {
                            "type": "OEE",
                            "id": None,
                            "RefWorkstation": {"type": "Relationship", "value": None},
                            "RefJob": {"type": "Relationship", "value": None},
                            "Availability": {"type": "Number", "value": None},
                            "Performance": {"type": "Number", "value": None},
                            "Quality": {"type": "Number", "value": None},
                            "OEE": {"type": "Number", "value": None}
                          }
    Throughput_template = {
                            "type": "Throughput",
                            "id": None,
                            "RefWorkstation": {"type": "Relationship", "value": None},
                            "RefJob": {"type": "Relationship", "value": None},
                            "ThroughputPerShift": {"type": "Number", "value": None}
                          }

    def __init__(self, workstation_id, operatorSchedule_id):
        self.logger = getLogger(__name__)
        self.oee = self.OEE_template
        self.throughput = self.Throughput_template
        self.today = {}

        self.operatorSchedule = self.object_.copy()
        self.operatorSchedule['id'] = operatorSchedule_id

        self.ws = self.object_.copy()
        self.ws['id'] = workstation_id
        self.ws['postgres_table'] = self.get_cygnus_postgres_table(self.ws['orion'])

        self.job = self.object_.copy()

        self.part = self.object_.copy()

        self.operation = self.object_.copy()

    def __repr__(self):
        return f'OEECalculator({self.ws["id"]})'

    def __str__(self):
        return f'OEECalculator object for workstation: {self.ws["id"]}'

    def get_cygnus_postgres_table(self, orion_obj):
        return orion_obj['id'].replace(':', '_').lower() + '_' + orion_obj['type'].lower()

    def msToDateTimeString(self, ms):
        return str(datetime.fromtimestamp(ms/1000.0).strftime(self.DATETIME_FORMAT))[:-3]

    def msToDateTime(self, ms):
        return self.stringToDateTime(self.msToDateTimeString(ms))

    def stringToDateTime(self, string):
        return datetime.strptime(string, self.DATETIME_FORMAT)
    
    def timeToDatetime(self, string):
        return datetime.strptime(str(self.now.date()) + ' ' + string, '%Y-%m-%d %H:%M:%S')

    def datetimeToMilliseconds(self, datetime_):
        return datetime_.timestamp()*1000

    def convertRecvtimetsToInt(self, df):
        df['recvtimets'] = df['recvtimets'].astype('float64').astype('int64')

    def get_operatorSchedule(self):
        self.operatorSchedule['orion'] = Orion.getObject(self.operatorSchedule['id'])
        self.logger.debug(f'OperatorSchedule: {self.operatorSchedule}')

    def is_datetime_in_todays_shift(self, datetime_):
        if datetime_ < self.today['OperatorWorkingScheduleStartsAt']:
            return False
        if datetime_ > self.today['OperatorWorkingScheduleStopsAt']:
            return False
        return True

    def get_attr_name_val(self, df_):
        return df_[['attrname', 'attrvalue']]

    def get_current_job_start_time(self):
        df = self.ws['df']
        # attr_name_val = self.get_attr_name_val(df)
        job_changes = df[df['attrname'] == 'RefJob']
        last_job = job_changes.iloc[-1]['attrvalue']
        if last_job != self.job['id']:
            raise ValueError(f'The last job in the Workstation object and the Workstation\'s PostgreSQL historic logs differ.\nWorkstation:\n{self.ws}\Last job in Workstation_logs:\n{last_job}')
        last_job_change = job_changes.iloc[-1]['recvtimets']
        return self.msToDateTime(last_job_change)

    def get_todays_shift_limits(self):
        try:
            for time_ in ('OperatorWorkingScheduleStartsAt', 'OperatorWorkingScheduleStopsAt'):
                self.today[time_] = self.timeToDatetime(self.operatorSchedule['orion'][time_]['value'])
        except (ValueError, KeyError) as error:
            raise ValueError(f'Critical: could not convert time in {self.operatorSchedule}. Traceback:\n{error}')
        current_job_start_time = self.get_current_job_start_time()
        if self.is_datetime_in_todays_shift(current_job_start_time):
            # the Job started in this shift, update RefStartTime
            self.today['RefStartTime'] = current_job_start_time
            self.logger.info(f'The current job started in this shift, updated RefStartTime: {self.today["RefStartTime"]}')
        else:
            self.today['RefStartTime'] = self.today['OperatorWorkingScheduleStartsAt']
            self.logger.info(f'The current job started before this shift, RefStartTime: {self.today["RefStartTime"]}')
        self.logger.debug(f'Today: {self.today}')

    def get_ws(self):
        self.ws['orion'] = Orion.getObject(self.ws['id'])
        self.logger.debug(f'Workstation: {self.ws}')

    def get_job_id(self):
        try:
            return self.ws['orion']['RefJob']['value']
        except (KeyError, TypeError) as error:
            raise AttributeError(f'The workstation object {self.ws["id"]} has no valid RefJob attribute:\nObject:\n{self.ws["orion"]}')

    def get_job(self):
        self.job['id'] = self.get_job_id()
        self.job['orion'] = Orion.getObject(self.job['id'])
        self.job['postgres_table'] = self.get_cygnus_postgres_table(self.job['orion'])
        self.logger.debug(f'Job: {self.job}')

    def get_part_id(self):
        try:
            part_id = self.job['orion']['RefPart']['value']
        except (KeyError, TypeError) as error:
            raise RuntimeError(f'Critical: RefPart not found in the Job {self.job["id"]}.\nObject:\n{self.job["orion"]}') from error
        self.part['id'] = part_id

    def get_part(self):
        self.part['id'] = self.get_part_id()
        self.part['orion'] = Orion.getObject(self.part['id'])
        self.logger.debug(f'Part: {self.part}')

    def get_operation(self):
        found = False
        try:
            for operation in self.part['orion']['Operations']['value']:
                if operation['OperationType']['value'] == self.job['CurrentOperationType']['value']:
                    found = True
                    self.operation['orion'] = operation
                    break
            if not found:
                raise KeyError(f'The part {self.part["orion"]} has no operation with type {self.job["CurrentOperationType"]}')
        except (AttributeError, KeyError) as error:
            raise KeyError(f'Invalid part or job specification. The current operation could not be resolved. See the JSON files for reference.\nJob:\n{self.job["orion"]}\nPart:\n{self.part["orion"]}') from error
        self.operation['id'] = self.operation['orion']['id']
        self.logger.debug(f'Operation: {self.operation}')

    def get_objects(self):
        self.get_operatorSchedule()
        self.get_todays_shift_limits()
        self.get_ws()
        self.get_job()
        self.get_part()
        self.get_operation()

    def download_ws_df(self, con):
        try:
            self.ws['df'] = pd.read_sql_query(f'''select * from {conf['postgresSchema']}.{self.ws['postgres_table']}
                                               where {self.datetimeToMilliseconds(self.today['start'])} < cast (recvtimets as bigint) 
                                               and cast (recvtimets as bigint) <= {self.now_unix};''', con=con)
        except (psycopg2.errors.UndefinedTable,
                sqlalchemy.exc.ProgrammingError) as error:
            raise RuntimeError(f'The SQL table: {self.ws["postgres_table"]} cannot be downloaded from the table_schema: {conf["postgresSchema"]}. Traceback:\n{error}') from error

    def download_job_df(self, con):
        try:
            self.job['df'] = pd.read_sql_query(f'''select * from {conf["postgresSchema"]}.{self.job["postgres_table"]}
                                                where {self.datetimeToMilliseconds(self.today["start"])} < cast (recvtimets as bigint)
                                                and cast (recvtimets as bigint) <= {self.now_unix};''', con=con)
        except (psycopg2.errors.UndefinedTable,
                sqlalchemy.exc.ProgrammingError) as error:
            raise RuntimeError(f'The SQL table: {self.job["postgres_table"]} cannot be downloaded from the table_schema: {conf["postgresSchema"]}. Traceback:\n{error}') from error

    def convert_dataframe_to_str(self, df):
        '''
        Cygnus 2.16.0 uploads all data as Text to Postgres
        So with this version of Cygnus, this function is useless
        We do this to ensure that we can always work with strings to increase stability
        '''
        return df.applymap(str)

    def prepare(self, con):
        self.now = datetime.now()
        self.now_unix = self.now.timestamp() * 1000
        self.today = {'day': self.now.date(),
                      'start': self.stringToDateTime(str(self.now.date()) + ' 00:00:00.000')}
        try:
            self.get_objects()
        except (RuntimeError, KeyError, AttributeError) as error:
            self.logger.error(f'Could not download and extract objects from Orion. Traceback:\n{error}')
            raise error

        if not isDateTimeInTodaysShift(self.now):
            raise ValueError(f'The current time: {self.now} is outside today\'s shift, no OEE data')

        self.download_ws_df(con)
        self.ws['df'] = self.convert_dataframe_to_str(self.ws['df'])
        self.convertRecvtimetsToInt(self.ws['df'])
        self.download_job_df(con)
        self.job['df'] = self.convert_dataframe_to_str(self.job['df'])
        self.convertRecvtimetsToInt(self.job['df'])

        self.oee['id'] = self.ws['orion']['RefOEE']['value']
        self.oee['RefWorkstation']['value'] = self.ws['id']
        self.oee['RefJob']['value'] = self.job['id']

        self.throughput['id'] = self.ws['orion']['RefThroughput']['value']
        self.throughput['RefWorkstation']['value'] = self.ws['id']
        self.throughput['RefJob']['value'] = self.job['id']

    def calc_availability(self):
        # Available is true and false in this periodical order, starting with true
        # we can sum the timestamps of the true values and the false values disctinctly, getting 2 sums
        # the total available time is their difference
        df = self.ws['df']
        df_av = df[df['attrname'] == 'Available']
        available_true = df_av[df_av['attrvalue'] == 'true']
        available_false = df_av[df_av['attrvalue'] == 'false']
        total_available_time = available_false['recvtimets'].sum() - available_true['recvtimets'].sum()
        # if the Workstation is available currently, we need to add
        # the current timestamp to the true timestamps' sum
        if (df_av.iloc[-1]['attrvalue'] == 'true'):
            total_available_time += self.now_unix
        self.total_available_time = total_available_time
        total_time_so_far_in_shift = self.datetimeToMilliseconds(self.now) - self.datetimeToMilliseconds(self.today['RefStartTime'])
        if total_time_so_far_in_shift == 0:
            raise ZeroDivisionError('Total time so far in the shift is 0, no OEE data')
        return total_available_time / total_time_so_far_in_shift

    def handle_availability(self):
        if self.ws['df'].size == 0:
            raise ValueError(f'No workstation data found for {self.ws["id"]} up to time {self.now} on day {self.today["day"]}, no OEE data')
        self.oee['Availability']['value'] = self.calc_availability()

    def count_nonzero_unique(self, unique_values):
        if '0' in unique_values:
            # need to substract 1, because '0' does not represent a successful moulding
            # for example: ['0', '8', '16', '24'] contains 4 unique values
            # but these mean only 3 successful injection mouldings
            return unique_values.shape[0] - 1
        else:
            return unique_values.shape[0]

    def count_injection_mouldings(self):
        df = self.job['df']
        attr_name_val = self.get_attr_name_val(df)
        good_unique_values = attr_name_val[attr_name_val['attrname'] == 'GoodPartCounter']['attrvalue'].unique()
        reject_unique_values = attr_name_val[attr_name_val['attrname'] == 'RejectPartCounter']['attrvalue'].unique()
        self.n_successful_mouldings = self.count_nonzero_unique(good_unique_values)
        self.n_failed_mouldings = self.count_nonzero_unique(reject_unique_values)
        self.n_total_mouldings = self.n_successful_mouldings + self.n_failed_mouldings

    def handle_quality(self):
        if self.job['df'].size == 0:
            raise ValueError(f'No job data found for {self.job["id"]} up to time {self.now} on day {self.today}, no OEE data')
        self.count_injection_mouldings()
        if self.n_total_mouldings == 0:
            raise ValueError('No operation was completed yet, no OEE data')
        self.oee['Quality']['value'] = self.n_successful_mouldings / self.n_total_mouldings

    def handle_performance(self):
        self.oee['Performance']['value'] = self.n_total_mouldings * self.operation['orion']['OperationTime']['value'] / self.total_available_time

    def calculate_OEE(self):
        self.handle_availability()
        self.handle_quality()
        self.handle_performance()
        self.oee['OEE']['value'] = self.oee['Availability']['value'] * self.oee['Performance']['value'] * self.oee['Quality']['value']
        self.logger.info(f'OEE data: {self.oee}')
        return self.oee

    def calculate_throughput(self):
        self.shiftLengthInMilliseconds = self.datetimeToMilliseconds(self.today['OperatorWorkingScheduleStopsAt']) - self.datetimeToMilliseconds(self.today['RefStartTime'])
        self.throughput['Throughput']['value'] = (self.shiftLengthInMilliseconds / self.operation['OperationTime']['value']) * self.operation['PartsPerOperation']['value'] * self.oee['OEE']['value']
        self.logger.info(f'Throughput: {self.throughput}')
        return self.throughput

