# Standard Library imports
import copy
from datetime import datetime
import json
import glob
import os
import sys
import unittest
from unittest.mock import patch

# PyPI imports
import pandas as pd
import numpy as np
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.types import DateTime, Float, BigInteger, Text

# Custom imports
sys.path.insert(0, os.path.join("..", "app"))

# Constants
WS_ID = "urn:ngsi_ld:Workstation:1"
WS_FILE = "urn_ngsi_ld_workstation_1_workstation.csv"
WS_TABLE = "urn_ngsi_ld_workstation_1_workstation"
OEE_TABLE = WS_TABLE + "_oee"
JOB_ID = "urn:ngsi_ld:Job:202200045"
JOB_FILE = "urn_ngsi_ld_job_202200045_job.csv"
JOB_TABLE = "urn_ngsi_ld_job_202200045_job"
PLACES = 4
COL_DTYPES = {
    "recvtimets": BigInteger(),
    "recvtime": DateTime(),
    "availability": Float(),
    "performance": Float(),
    "quality": Float(),
    "oee": Float(),
    "throughput_shift": Float(),
    "job": Text(),
}

from OEE import OEECalculator
from Logger import getLogger
from modules import reupload_jsons_to_Orion
from modules.remove_orion_metadata import remove_orion_metadata

# Load environment variables
POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_SCHEMA = os.environ.get("POSTGRES_SCHEMA")


class test_OEECalculator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.logger = getLogger(__name__)
        cls.maxDiff = None
        reupload_jsons_to_Orion.main()
        cls.engine = create_engine(
            f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}"
        )
        cls.con = cls.engine.connect()
        if not cls.engine.dialect.has_schema(cls.engine, POSTGRES_SCHEMA):
            cls.engine.execute(sqlalchemy.schema.CreateSchema(POSTGRES_SCHEMA))

        cls.oee_template = OEECalculator(WS_ID)

        # def remove_trailing_dot_0(str_):
        #     # remove trailing '.0'-s from str representation on integers
        #     # e.g. '110.0' -> '110'
        #     return str_.split('.')[0]

        # read and upload both tables to PostgreSQL
        # then download them to ensure that the data types
        # match the data types in production
        cls.ws_df = pd.read_csv(os.path.join("csv", WS_FILE))
        cls.ws_df["recvtimets"] = cls.ws_df["recvtimets"].map(int)
        cls.ws_df.to_sql(
            name=WS_TABLE,
            con=cls.con,
            schema=POSTGRES_SCHEMA,
            index=False,
            dtype=Text,
            if_exists="replace",
        )
        cls.ws_df = pd.read_sql_query(
            f"select * from {POSTGRES_SCHEMA}.{WS_TABLE}", con=cls.con
        )

        cls.job_df = pd.read_csv(os.path.join("csv", JOB_FILE))
        cls.job_df["recvtimets"] = cls.job_df["recvtimets"].map(int)
        cls.job_df.to_sql(
            name=JOB_TABLE,
            con=cls.con,
            schema=POSTGRES_SCHEMA,
            index=False,
            dtype=Text,
            if_exists="replace",
        )
        cls.job_df = pd.read_sql_query(
            f"select * from {POSTGRES_SCHEMA}.{JOB_TABLE}", con=cls.con
        )

        cls.jsons = {}
        jsons = glob.glob(os.path.join("..", "json", "*.json"))
        for file in jsons:
            json_name = os.path.splitext(os.path.basename(file))[0]
            with open(file, "r") as f:
                cls.jsons[json_name] = json.load(f)

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.oee = copy.deepcopy(self.oee_template)

    def tearDown(self):
        pass

    def test_set_now(self):
        now = datetime.now()
        self.oee.set_now()
        self.assertAlmostEqual(now.timestamp(), self.oee.now.timestamp(), places=PLACES)

    def test_now_unix(self):
        self.oee.now = datetime(2022, 4, 5, 13, 46, 40)
        # using GMT+2 time zone
        self.assertEqual(self.oee.now_unix(), 1649159200000)

    def test_msToDateTimeString(self):
        self.assertEqual(
            self.oee.msToDateTimeString(1649159200000), "2022-04-05 13:46:40.000"
        )

    def test_msToDateTime(self):
        self.assertEqual(
            self.oee.msToDateTime(1649159200000), datetime(2022, 4, 5, 13, 46, 40)
        )

    def test_stringToDateTime(self):
        self.assertEqual(
            self.oee.stringToDateTime("2022-04-05 13:46:40.000"),
            datetime(2022, 4, 5, 13, 46, 40),
        )

    def test_timeToDatetime(self):
        self.oee.now = datetime(2022, 4, 5, 15, 26, 0)
        self.assertEqual(
            self.oee.timeToDatetime("13:46:40"), datetime(2022, 4, 5, 13, 46, 40)
        )

    def test_datetimeToMilliseconds(self):
        self.assertEqual(
            self.oee.datetimeToMilliseconds(datetime(2022, 4, 5, 13, 46, 40)),
            1649159200000,
        )

    def test_convertRecvtimetsToInt(self):
        self.oee.ws["df"] = self.ws_df.copy()
        self.oee.convertRecvtimetsToInt(self.oee.ws["df"])
        self.assertEqual(self.oee.ws["df"]["recvtimets"].dtype, np.int64)

    def test_get_cygnus_postgres_table(self):
        job_table = self.oee.get_cygnus_postgres_table(self.jsons["Job202200045"])
        self.assertEqual(job_table, "urn_ngsi_ld_job_202200045_job")

    def test_get_ws(self):
        self.oee.get_ws()
        self.assertEqual(
            remove_orion_metadata(self.oee.ws["orion"]), self.jsons["Workstation"]
        )
        self.assertEqual(
            self.oee.ws["postgres_table"], "urn_ngsi_ld_workstation_1_workstation"
        )

    def test_get_operatorSchedule(self):
        self.oee.ws["orion"] = copy.deepcopy(self.jsons["Workstation"])
        self.oee.get_operatorSchedule()
        self.assertEqual(
            remove_orion_metadata(self.oee.operatorSchedule["orion"]),
            self.jsons["OperatorSchedule"],
        )
        self.oee.ws["orion"] = copy.deepcopy(self.jsons["Workstation"])
        self.oee.ws["orion"]["RefOperatorSchedule"] = "invalid_operationSchedule:id"
        with self.assertRaises(KeyError):
            self.oee.get_operatorSchedule()

    def test_is_datetime_in_todays_shift(self):
        self.oee.today["OperatorWorkingScheduleStartsAt"] = datetime(
            2022, 4, 4, 8, 0, 0
        )
        self.oee.today["OperatorWorkingScheduleStopsAt"] = datetime(
            2022, 4, 4, 16, 0, 0
        )
        dt1 = datetime(2022, 4, 4, 9, 0, 0)
        self.assertTrue(self.oee.is_datetime_in_todays_shift(dt1))
        dt2 = datetime(2022, 4, 4, 7, 50, 0)
        self.assertFalse(self.oee.is_datetime_in_todays_shift(dt2))
        dt3 = datetime(2022, 4, 4, 16, 10, 0)
        self.assertFalse(self.oee.is_datetime_in_todays_shift(dt3))

    def test_get_todays_shift_limits(self):
        self.oee.now = datetime(2022, 8, 23, 13, 0, 0)
        self.oee.operatorSchedule["orion"] = copy.deepcopy(
            self.jsons["OperatorSchedule"]
        )
        self.oee.get_todays_shift_limits()
        self.assertEqual(
            self.oee.today["OperatorWorkingScheduleStartsAt"],
            datetime(2022, 8, 23, 8, 0, 0),
        )
        self.assertEqual(
            self.oee.today["OperatorWorkingScheduleStopsAt"],
            datetime(2022, 8, 23, 16, 0, 0),
        )

    def test_get_job_id(self):
        self.oee.ws["orion"] = copy.deepcopy(self.jsons["Workstation"])
        self.assertEqual(self.oee.get_job_id(), "urn:ngsi_ld:Job:202200045")
        self.oee.ws["orion"]["RefJob"] = None
        with self.assertRaises(KeyError):
            self.oee.get_job_id()

    def test_get_job(self):
        self.oee.ws["orion"] = copy.deepcopy(self.jsons["Workstation"])
        # self.oee.job['id'] = 'urn:ngsi_ld:Job:202200045'
        self.oee.get_job()
        self.assertEqual(
            remove_orion_metadata(self.oee.job["orion"]), self.jsons["Job202200045"]
        )
        self.assertEqual(
            self.oee.job["postgres_table"], "urn_ngsi_ld_job_202200045_job"
        )

    def test_get_part_id(self):
        self.oee.job["orion"] = copy.deepcopy(self.jsons["Job202200045"])
        self.oee.get_part_id()
        self.assertEqual(self.oee.part["id"], "urn:ngsi_ld:Part:Core001")
        self.oee.job["orion"]["RefPart"] = "invalid"
        with self.assertRaises(KeyError):
            self.oee.get_part_id()

    def test_get_part(self):
        self.oee.job["orion"] = copy.deepcopy(self.jsons["Job202200045"])
        self.logger.debug(f'oee.job["orion"]: {self.oee.job["orion"]}')
        # print(f'oee.job["orion"]: {self.oee.job["orion"]}')
        self.oee.get_part()
        self.assertEqual(
            remove_orion_metadata(self.oee.part["orion"]), self.jsons["Core001"]
        )

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
                        "OperationType": {
                            "type": "Text",
                            "value": "Core001_injection_moulding",
                        },
                        "PartsPerOperation": {"type": "Number", "value": 8},
                    },
                    {
                        "type": "Operation",
                        "OperationNumber": {"type": "Number", "value": 20},
                        "OperationTime": {"type": "Number", "value": 33},
                        "OperationType": {"type": "Text", "value": "Core001_deburring"},
                        "PartsPerOperation": {"type": "Number", "value": 16},
                    },
                ],
            },
        }
        self.oee.job["orion"] = copy.deepcopy(self.jsons["Job202200045"])
        self.oee.part["orion"] = part
        self.oee.get_operation()
        self.assertEqual(self.oee.operation["orion"], part["Operations"]["value"][0])

        self.oee.job["orion"]["CurrentOperationType"]["value"] = "Core001_painting"
        with self.assertRaises(KeyError):
            self.oee.get_operation()

    def test_get_objects_shift_limits(self):
        self.oee.now = datetime(2022, 8, 23, 13, 0, 0)
        # self.oee.now_unix = self.oee.now.timestamp() * 1000
        self.oee.get_objects_shift_limits()
        self.assertEqual(
            remove_orion_metadata(self.oee.ws["orion"]), self.jsons["Workstation"]
        )
        self.assertEqual(
            remove_orion_metadata(self.oee.operatorSchedule["orion"]),
            self.jsons["OperatorSchedule"],
        )
        self.assertEqual(
            self.oee.today["OperatorWorkingScheduleStartsAt"],
            datetime(2022, 8, 23, 8, 0, 0),
        )
        self.assertEqual(
            self.oee.today["OperatorWorkingScheduleStopsAt"],
            datetime(2022, 8, 23, 16, 0, 0),
        )
        self.assertEqual(
            remove_orion_metadata(self.oee.job["orion"]), self.jsons["Job202200045"]
        )
        self.assertEqual(
            remove_orion_metadata(self.oee.part["orion"]), self.jsons["Core001"]
        )
        self.assertEqual(
            remove_orion_metadata(self.oee.operation["orion"]),
            self.jsons["Core001"]["Operations"]["value"][0],
        )

    def test_get_query_start_timestamp(self):
        self.oee.now = datetime(2022, 4, 5, 13, 0, 0)
        self.oee.operatorSchedule["orion"] = copy.deepcopy(self.jsons["OperatorSchedule"])
        self.oee.get_todays_shift_limits()
        self.assertEqual(
            self.oee.get_query_start_timestamp(how="from_midnight"),
            self.oee.datetimeToMilliseconds(datetime(2022, 4, 5, 0, 0, 0))
        )
        self.assertEqual(
            self.oee.get_query_start_timestamp(how="from_schedule_start"),
            self.oee.datetimeToMilliseconds(datetime(2022, 4, 5, 8, 0, 0))
        )

    def test_query_todays_data(self):
        self.oee.now = datetime(2022, 4, 4, 13, 0, 0)
        self.oee.operatorSchedule["orion"] = copy.deepcopy(self.jsons["OperatorSchedule"])
        self.oee.get_objects_shift_limits()
        self.oee.ws["df"] = self.oee.query_todays_data(
            self.con, self.oee.ws["postgres_table"], how="from_midnight"
        )
        df = self.ws_df.copy()
        df["recvtimets"] = df["recvtimets"].map(str).map(int)
        df.dropna(how="any", inplace=True)
        start_timestamp = self.oee.datetimeToMilliseconds(
            datetime(2022, 4, 4)
        )
        df = df[(start_timestamp <= df["recvtimets"]) & (df["recvtimets"] <= self.oee.now_unix())]
        df["recvtimets"] = df["recvtimets"].map(str)
        self.oee.ws["df"].dropna(how="any", inplace=True)
        df.dropna(how="any", inplace=True)
        self.assertTrue(self.oee.ws["df"].equals(df))

        self.oee.ws["df"] = self.oee.query_todays_data(
            self.con, self.oee.ws["postgres_table"], how="from_schedule_start"
        )
        df = self.ws_df.copy()
        df["recvtimets"] = df["recvtimets"].map(str).map(int)
        df.dropna(how="any", inplace=True)
        start_timestamp = self.oee.datetimeToMilliseconds(
            datetime(2022, 4, 4, 8, 0, 0)
        )
        df = df[(start_timestamp <= df["recvtimets"]) & (df["recvtimets"] <= self.oee.now_unix())]
        df["recvtimets"] = df["recvtimets"].map(str)
        df.reset_index(inplace=True, drop=True)
        # self.oee.ws["df"].dropna(how="any", inplace=True)
        # df.dropna(how="any", inplace=True)
        self.oee.ws["df"].dtypes.to_csv("oee_ws_df_dtype.csv")
        df.dtypes.to_csv("calculated_df_dtype.csv")
        self.oee.ws["df"].to_csv("oee_ws_df.csv")
        df.to_csv("calculated_df.csv")
        self.assertTrue(self.oee.ws["df"].equals(df))

        with patch("pandas.read_sql_query") as mocked_read_sql_query:
            mocked_read_sql_query.side_effect = psycopg2.errors.UndefinedTable
            with self.assertRaises(RuntimeError):
                self.oee.ws["df"] = self.oee.query_todays_data(
                    self.con, self.oee.ws["postgres_table"], how="from_midnight"
                )

    def test_get_current_job_start_time_today(self):
        self.oee.now = datetime(2022, 4, 4, 13, 0, 0)
        self.oee.operatorSchedule["orion"] = copy.deepcopy(self.jsons["OperatorSchedule"])
        self.oee.get_todays_shift_limits()
        self.oee.job["id"] = "urn:ngsi_ld:Job:202200045"
        self.oee.ws["orion"] = copy.deepcopy(self.jsons["Workstation"])
        self.oee.ws["postgres_table"] = self.oee.get_cygnus_postgres_table(self.oee.ws["orion"])
        ws_df = self.oee.query_todays_data(self.con, self.oee.ws["postgres_table"], how="from_midnight")
        ws_df["recvtimets"] = ws_df["recvtimets"].map(str).map(float).map(int)
        self.oee.ws["df"] = ws_df.copy()
        # self.oee.convertRecvtimetsToInt(self.oee.ws["df"])
        self.assertEqual(
            self.oee.get_current_job_start_time_today(), datetime(2022, 4, 4, 8, 0, 0)
        )

        dt_at_9h = datetime(2022, 4, 4, 9, 0, 0)
        ts_at_9h = dt_at_9h.timestamp() * 1000
        ws_df.loc[len(ws_df)] = [
            int(ts_at_9h),
            self.oee.msToDateTimeString(ts_at_9h),
            "/",
            "urn:ngsi_ld:Workstation:1",
            "Workstation",
            "RefJob",
            "Text",
            "urn:ngsi_ld:Job:202200045",
            "[]"
        ]
        ws_df.sort_values(by=["recvtimets"], inplace=True)

        self.oee.ws["df"] = ws_df.copy()
        self.assertEqual(self.oee.get_current_job_start_time_today(), dt_at_9h)

        dt_at_10h = datetime(2022, 4, 4, 10, 0, 0)
        ts_at_10h = dt_at_10h.timestamp() * 1000
        ws_df.loc[len(ws_df)] = [
            int(ts_at_10h),
            self.oee.msToDateTimeString(ts_at_10h),
            "/",
            "urn:ngsi_ld:Workstation:1",
            "Workstation",
            "RefJob",
            "Text",
            "urn:ngsi_ld:Job:202200046",
            "[]",
        ]
        self.oee.ws["df"] = ws_df.copy()
        with self.assertRaises(ValueError):
            self.oee.get_current_job_start_time_today()
        # row_to_be_inserted = pd.DataFrame.from_dict({'recvtimets': [str(ts_at_9h) + '.0'],
        #                                              'recvtime': [self.oee.msToDateTimeString(ts_at_9h)],
        #                                              'fiwareservicepath': ['/'],
        #                                              'entityid': ['urn:ngsi_ld:Workstation:1'],
        #                                              'entitytype': ['Workstation'],
        #                                              'attrname': ['RefJob'],
        #                                              'attrvalue': ['urn:ngsi_ld:Job:202200045'],
        #                                              'attrmd': ['[]']})

    def test_setRefStartTime(self):
        pass
        # current_job_start_time = self.get_current_job_start_time_today()
        # if self.is_datetime_in_todays_shift(current_job_start_time):
        #     # the Job started in this shift, update RefStartTime
        #     self.today['RefStartTime'] = current_job_start_time
        #     self.logger.info(f'The current job started in this shift, updated RefStartTime: {self.today["RefStartTime"]}')
        # else:
        #     self.today['RefStartTime'] = self.today['OperatorWorkingScheduleStartsAt']
        #     self.logger.info(f'The current job started before this shift, RefStartTime: {self.today["RefStartTime"]}')

    def test_convert_dataframe_to_str(self):
        """
        Cygnus 2.16.0 uploads all data as Text to Postgres
        So with this version of Cygnus, this function is useless
        We do this to ensure that we can always work with strings to increase stability
        """
        pass
        # return df.applymap(str)

    def test_sort_df_by_time(self):
        pass
        # default: ascending order
        # if df_['recvtimets'].dtype != np.int64:
        #     raise ValueError(f'The recvtimets column should contain np.int64 dtype values, current dtype: {df_["recvtimets"]}')
        # return df_.sort_values(by=['recvtimets'])

    def test_prepare(self):
        pass
        # self.now = datetime.now()
        # self.now_unix() = self.now.timestamp() * 1000
        # self.today = {'day': self.now.date(),
        #               'start': self.stringToDateTime(str(self.now.date()) + ' 00:00:00.000')}
        # try:
        #     self.get_objects_shift_limits()
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
        #     total_available_time += self.now_unix()
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

    def test_count_nonzero_unique(self):
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


if __name__ == "__main__":
    try:
        unittest.main()
    except Exception as error:
        print(error)
