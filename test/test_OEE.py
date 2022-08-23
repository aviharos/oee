# Standard Library imports
import copy
import datetime
import json
import glob
import os
import sys
import unittest
from unittest.mock import patch

# PyPI imports
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.types import DateTime, Float, BigInteger, Text

# Custom imports
sys.path.insert(0, os.path.join('..', 'app'))

# Constants
WS_ID = 'urn:ngsi_ld:Workstation:1'
WS_FILE = 'urn_ngsi_ld_workstation_1_workstation.csv'
WS_TABLE = 'urn_ngsi_ld_workstation_1_workstation'
OEE_TABLE = WS_TABLE + '_oee'
JOB_ID = 'urn:ngsi_ld:Job:202200045'
JOB_FILE = 'urn_ngsi_ld_job_202200045_job.csv'
JOB_TABLE = 'urn_ngsi_ld_job_202200045_job'
PLACES = 4
COL_DTYPES = {'recvtimets': BigInteger(),
              'recvtime': DateTime(),
              'availability': Float(),
              'performance': Float(),
              'quality': Float(),
              'oee': Float(),
              'throughput_shift': Float(),
              'job': Text()}

from OEE import OEECalculator
from modules import reupload_jsons_to_Orion
from modules.remove_orion_metadata import remove_orion_metadata

# Load environment variables
POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT')
POSTGRES_USER = os.environ.get('POSTGRES_USER')
POSTGRES_SCHEMA = os.environ.get('POSTGRES_SCHEMA')


class testOrion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        reupload_jsons_to_Orion.main()
        cls.engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}')
        cls.con = cls.engine.connect()
        if not cls.engine.dialect.has_schema(cls.engine, POSTGRES_SCHEMA):
            cls.engine.execute(sqlalchemy.schema.CreateSchema(POSTGRES_SCHEMA))

        cls.oee_template = OEECalculator(WS_ID)

        # read and upload both tables to PostgreSQL
        # then download them to ensure that the data types
        # match the data types in production
        cls.ws_df = pd.read_csv(os.path.join('csv', WS_FILE))
        cls.ws_df.to_sql(name=WS_TABLE, con=cls.con, schema=POSTGRES_SCHEMA, index=False, dtype=Text, if_exists='replace')
        cls.ws_df = pd.read_sql_query(f'select * from {POSTGRES_SCHEMA}.{WS_TABLE}', con=cls.con)

        cls.job_df = pd.read_csv(os.path.join('csv', JOB_FILE))
        cls.job_df.to_sql(name=JOB_TABLE, con=cls.con, schema=POSTGRES_SCHEMA, index=False, dtype=Text, if_exists='replace')
        cls.job_df = pd.read_sql_query(f'select * from {POSTGRES_SCHEMA}.{JOB_TABLE}', con=cls.con)

        cls.jsons = {}
        jsons = glob.glob(os.path.join('..', 'json', '*.json'))
        for file in jsons:
            json_name = os.path.splitext(os.path.basename(file))[0]
            with open(file, 'r') as f:
                cls.jsons[json_name] = json.load(f)

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.oee = copy.deepcopy(self.oee_template)

    def tearDown(self):
        pass

    def test_get_cygnus_postgres_table(self):
        job_table = self.oee.get_cygnus_postgres_table(self.jsons['Job202200045'])
        self.assertEqual(job_table, "urn_ngsi_ld_job_202200045_job")

    def test_msToDateTimeString(self):
        self.assertEqual(self.oee.msToDateTimeString(1649159200000), '2022-04-05 13:46:40.000')

    def test_msToDateTime(self):
        self.assertEqual(self.oee.msToDateTime(1649159200000), datetime.datetime(2022, 4, 5, 13, 46, 40))

    def test_stringToDateTime(self):
        self.assertEqual(self.oee.stringToDateTime('2022-04-05 13:46:40.000'), datetime.datetime(2022, 4, 5, 13, 46, 40))
    
    def test_timeToDatetime(self):
        self.oee.now = datetime.datetime(2022, 4, 5, 15, 26, 0)
        self.assertEqual(self.oee.timeToDatetime('13:46:40'), datetime.datetime(2022, 4, 5, 13, 46, 40))

    def test_datetimeToMilliseconds(self):
        self.assertEqual(self.oee.datetimeToMilliseconds(datetime.datetime(2022, 4, 5, 13, 46, 40)), 1649159200000)

    def test_convertRecvtimetsToInt(self):
        self.oee.ws['df'] = self.ws_df.copy()
        self.oee.convertRecvtimetsToInt(self.oee.ws['df'])
        self.assertEqual(self.oee.ws['df']['recvtimets'].dtype, np.int64)

    def test_get_ws(self):
        pass
        # self.ws['orion'] = Orion.getObject(self.ws['id'])
        # self.ws['postgres_table'] = self.get_cygnus_postgres_table(self.ws['orion'])
        # self.logger.debug(f'Workstation: {self.ws}')

    def test_get_operatorSchedule(self):
        pass
        # try:
        #     self.operatorSchedule['id'] = self.ws['orion']['RefOperatorSchedule']['value']
        # except KeyError as error:
        #     raise KeyError(f'Critical: RefOperatorSchedule not foundin Workstation object :\n{self.ws["orion"]}.') from error
        # self.operatorSchedule['orion'] = Orion.getObject(self.operatorSchedule['id'])
        # self.logger.debug(f'OperatorSchedule: {self.operatorSchedule}')

    def test_is_datetime_in_todays_shift(self, datetime_):
        pass
        # if datetime_ < self.today['OperatorWorkingScheduleStartsAt']:
        #     return False
        # if datetime_ > self.today['OperatorWorkingScheduleStopsAt']:
        #     return False
        # return True

    def test_get_todays_shift_limits(self):
        pass
        # try:
        #     for time_ in ('OperatorWorkingScheduleStartsAt', 'OperatorWorkingScheduleStopsAt'):
        #         self.today[time_] = self.timeToDatetime(self.operatorSchedule['orion'][time_]['value'])
        # except (ValueError, KeyError) as error:
        #     raise ValueError(f'Critical: could not convert time in {self.operatorSchedule}.') from error
        # self.logger.debug(f'Today: {self.today}')

    def test_get_job_id(self):
        pass
        # try:
        #     return self.ws['orion']['RefJob']['value']
        # except (KeyError, TypeError) as error:
        #     raise AttributeError(f'The workstation object {self.ws["id"]} has no valid RefJob attribute:\nObject:\n{self.ws["orion"]}')

    def test_get_job(self):
        pass
        # self.job['id'] = self.get_job_id()
        # self.job['orion'] = Orion.getObject(self.job['id'])
        # self.job['postgres_table'] = self.get_cygnus_postgres_table(self.job['orion'])
        # self.logger.debug(f'Job: {self.job}')

    def test_get_part_id(self):
        pass
        # try:
        #     part_id = self.job['orion']['RefPart']['value']
        # except (KeyError, TypeError) as error:
        #     raise RuntimeError(f'Critical: RefPart not found in the Job {self.job["id"]}.\nObject:\n{self.job["orion"]}') from error
        # self.part['id'] = part_id

    def test_get_part(self):
        pass
        # self.part['id'] = self.get_part_id()
        # self.part['orion'] = Orion.getObject(self.part['id'])
        # self.logger.debug(f'Part: {self.part}')

    def test_get_operation(self):
        part = {
                "type": "Part",
                "id": "urn:ngsi_ld:Part:Core001",
                "Operations": {
                    "type": "List",
                    "value": [
                        {
                            "type": "Operation",
                            "OperationNumber": {"type": "Number", "value": 10},
                            "OperationTime": {"type": "Number", "value": 46},
                            "OperationType": {"type": "Text", "value": "Core001_injection_moulding"},
                            "PartsPerOperation": {"type": "Number", "value": 8}
                        },
                        {
                            "type": "Operation",
                            "OperationNumber": {"type": "Number", "value": 20},
                            "OperationTime": {"type": "Number", "value": 33},
                            "OperationType": {"type": "Text", "value": "Core001_deburring"},
                            "PartsPerOperation": {"type": "Number", "value": 16}
                        },
                    ]
                }
            }
        op = part['Operations']['value'][0]
        self.assertEqual(self.oee.get_operation(part, 'Core001_injection_moulding'), op)
        with self.assertRaises(AttributeError):
            self.oee.get_operation(part, 'Core001_painting')

    def test_get_objects(self):
        pass
        # self.get_ws()
        # self.get_operatorSchedule()
        # self.get_todays_shift_limits()
        # self.get_job()
        # self.get_part()
        # self.get_operation()

    def test_download_todays_data_df(self, con, table_name):
        pass
        # try:
        #     df = pd.read_sql_query(f'''select * from {self.POSTGRES_SCHEMA}.{table_name}
        #                                where {self.datetimeToMilliseconds(self.today['RefStartTime'])} < cast (recvtimets as bigint) 
        #                                and cast (recvtimets as bigint) <= {self.now_unix};''', con=con)
        # except (psycopg2.errors.UndefinedTable,
        #         sqlalchemy.exc.ProgrammingError) as error:
        #     raise RuntimeError(f'The SQL table: {table_name} cannot be downloaded from the table_schema: {self.POSTGRES_SCHEMA}.') from error
        # return df

    def test_convert_dataframe_to_str(self, df):
        '''
        Cygnus 2.16.0 uploads all data as Text to Postgres
        So with this version of Cygnus, this function is useless
        We do this to ensure that we can always work with strings to increase stability
        '''
        pass
        # return df.applymap(str)

    def test_sort_df_by_time(self, df_):
        pass
        # default: ascending order
        # if df_['recvtimets'].dtype != np.int64:
        #     raise ValueError(f'The recvtimets column should contain np.int64 dtype values, current dtype: {df_["recvtimets"]}')
        # return df_.sort_values(by=['recvtimets'])

    def test_get_current_job_start_time_today(self):
        '''
        If the the current job started in today's shift,
        return its start time,
        else return the shift's start time
        '''
        pass
        # df = self.ws['df']
        # job_changes = df[df['attrname'] == 'RefJob']
        #
        # if len(job_changes) == 0:
        #     # today's downloaded ws df does not contain a job change
        #     return self.today['OperatorWorkingScheduleStartsAt']
        # last_job = job_changes.iloc[-1]['attrvalue']
        # if last_job != self.job['id']:
        #     raise ValueError(f'The last job in the Workstation object and the Workstation\'s PostgreSQL historic logs differ.\nWorkstation:\n{self.ws}\Last job in Workstation_logs:\n{last_job}')
        # last_job_change = job_changes.iloc[-1]['recvtimets']
        # return self.msToDateTime(last_job_change)

    def test_setRefStartTime(self):
        pass
        # current_job_start_time = self.get_current_job_start_time()
        # if self.is_datetime_in_todays_shift(current_job_start_time):
        #     # the Job started in this shift, update RefStartTime
        #     self.today['RefStartTime'] = current_job_start_time
        #     self.logger.info(f'The current job started in this shift, updated RefStartTime: {self.today["RefStartTime"]}')
        # else:
        #     self.today['RefStartTime'] = self.today['OperatorWorkingScheduleStartsAt']
        #     self.logger.info(f'The current job started before this shift, RefStartTime: {self.today["RefStartTime"]}')

    def test_prepare(self, con):
        pass
        # self.now = datetime.now()
        # self.now_unix = self.now.timestamp() * 1000
        # self.today = {'day': self.now.date(),
        #               'start': self.stringToDateTime(str(self.now.date()) + ' 00:00:00.000')}
        # try:
        #     self.get_objects()
        # except (RuntimeError, KeyError, AttributeError) as error:
        #     message = f'Could not download and extract objects from Orion. Traceback:\n{error}'
        #     self.logger.error(message)
        #     raise RuntimeError(message) from error
        #
        # if not self.is_datetime_in_todays_shift(self.now):
        #     raise ValueError(f'The current time: {self.now} is outside today\'s shift, no OEE data')
        #
        # self.ws['df'] = self.download_todays_data_df(con, self.ws['postgres_table'])
        # self.ws['df'] = self.convert_dataframe_to_str(self.ws['df'])
        # self.convertRecvtimetsToInt(self.ws['df'])
        # self.ws['df'] = self.sort_df_by_time(self.ws['df'])
        #
        # self.job['df'] = self.download_todays_data_df(con, self.job['postgres_table'])
        # self.job['df'] = self.convert_dataframe_to_str(self.job['df'])
        # self.convertRecvtimetsToInt(self.job['df'])
        # self.job['df'] = self.sort_df_by_time(self.job['df'])
        #
        # self.oee['id'] = self.ws['orion']['RefOEE']['value']
        # self.oee['RefWorkstation']['value'] = self.ws['id']
        # self.oee['RefJob']['value'] = self.job['id']
        #
        # self.throughput['id'] = self.ws['orion']['RefThroughput']['value']
        # self.throughput['RefWorkstation']['value'] = self.ws['id']
        # self.throughput['RefJob']['value'] = self.job['id']
        #
        # self.setRefStartTime()

    def test_calc_availability(self):
        pass
        # # Available is true and false in this periodical order, starting with true
        # # we can sum the timestamps of the true values and the false values disctinctly, getting 2 sums
        # # the total available time is their difference
        # df = self.ws['df']
        # df_av = df[df['attrname'] == 'Available']
        # available_true = df_av[df_av['attrvalue'] == 'true']
        # available_false = df_av[df_av['attrvalue'] == 'false']
        # total_available_time = available_false['recvtimets'].sum() - available_true['recvtimets'].sum()
        # # if the Workstation is available currently, we need to add
        # # the current timestamp to the true timestamps' sum
        # if (df_av.iloc[-1]['attrvalue'] == 'true'):
        #     total_available_time += self.now_unix
        # self.total_available_time = total_available_time
        # total_time_so_far_in_shift = self.datetimeToMilliseconds(self.now) - self.datetimeToMilliseconds(self.today['RefStartTime'])
        # if total_time_so_far_in_shift == 0:
        #     raise ZeroDivisionError('Total time so far in the shift is 0, no OEE data')
        # return total_available_time / total_time_so_far_in_shift

    def test_handle_availability(self):
        pass
        # if self.ws['df'].size == 0:
        #     raise ValueError(f'No workstation data found for {self.ws["id"]} up to time {self.now} on day {self.today["day"]}, no OEE data')
        # self.oee['Availability']['value'] = self.calc_availability()

    def test_count_nonzero_unique(self, unique_values):
        pass
        # if '0' in unique_values:
        #     # need to substract 1, because '0' does not represent a successful moulding
        #     # for example: ['0', '8', '16', '24'] contains 4 unique values
        #     # but these mean only 3 successful injection mouldings
        #     return unique_values.shape[0] - 1
        # else:
        #     return unique_values.shape[0]

    def test_count_injection_mouldings(self):
        pass
        # df = self.job['df']
        # attr_name_val = df[['attrname', 'attrvalue']]
        # good_unique_values = attr_name_val[attr_name_val['attrname'] == 'GoodPartCounter']['attrvalue'].unique()
        # reject_unique_values = attr_name_val[attr_name_val['attrname'] == 'RejectPartCounter']['attrvalue'].unique()
        # self.n_successful_mouldings = self.count_nonzero_unique(good_unique_values)
        # self.n_failed_mouldings = self.count_nonzero_unique(reject_unique_values)
        # self.n_total_mouldings = self.n_successful_mouldings + self.n_failed_mouldings

    def test_handle_quality(self):
        pass
        # if self.job['df'].size == 0:
        #     raise ValueError(f'No job data found for {self.job["id"]} up to time {self.now} on day {self.today}, no OEE data')
        # self.count_injection_mouldings()
        # if self.n_total_mouldings == 0:
        #     raise ValueError('No operation was completed yet, no OEE data')
        # self.oee['Quality']['value'] = self.n_successful_mouldings / self.n_total_mouldings

    def test_handle_performance(self):
        pass
        # self.oee['Performance']['value'] = self.n_total_mouldings * self.operation['orion']['OperationTime']['value'] / self.total_available_time

    def test_calculate_OEE(self):
        pass
        # self.handle_availability()
        # self.handle_quality()
        # self.handle_performance()
        # self.oee['OEE']['value'] = self.oee['Availability']['value'] * self.oee['Performance']['value'] * self.oee['Quality']['value']
        # self.logger.info(f'OEE data: {self.oee}')
        # return self.oee


if __name__ == '__main__':
    try:
        unittest.main()
    except Exception as error:
        print(error)

