# Standard Library imports
import copy
from datetime import datetime
import json
import glob
import os
import sys
from typing import Type
import unittest
from unittest.mock import patch

# PyPI imports
import pandas as pd
import numpy as np
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.types import Text

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

# from OEE import OEECalculator
import OEE
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

        cls.oee_template = OEE.OEECalculator(WS_ID)

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
        cls.con.close()
        cls.engine.dispose()

    @classmethod
    def prepare_df_between(cls, df, start, end):
        cls.logger.info(f"start: {start}")
        cls.logger.info(f"end: {end}")
        df = df.applymap(str)
        df["recvtimets"] = df["recvtimets"].astype("float64").astype("int64")
        df.sort_values(by=["recvtimets"], inplace=True)
        cls.logger.info(f"df before drop:\n{df}")
        df.dropna(how="any", inplace=True)
        cls.logger.info(f"df after drop:\n{df}")
        start_timestamp = start.timestamp() * 1000
        end_timestamp = end.timestamp() * 1000
        cls.logger.info(f"start_timestamp: {start_timestamp}")
        cls.logger.info(f"end timestamp: {end_timestamp}")
        df = df[
            (start_timestamp <= df["recvtimets"]) & (df["recvtimets"] <= end_timestamp)
        ].reset_index(drop=True)
        return df

    @classmethod
    def write_df_with_dtypes(cls, df, name):
        df.to_csv(f"{name}_val.csv")
        df.dtypes.to_csv(f"{name}_dtypes.csv")

    @classmethod
    def are_dfs_equal(cls, df1, df2):
        df1_sorted = df1.sort_values(by=["recvtimets", "attrname"]).reset_index(
            drop=True
        )
        df2_sorted = df2.sort_values(by=["recvtimets", "attrname"]).reset_index(
            drop=True
        )
        return df1_sorted.equals(df2_sorted)

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

        del self.oee.ws["orion"]["RefOperatorSchedule"]["value"]
        with self.assertRaises(KeyError):
            self.oee.get_operatorSchedule()

        self.oee.ws["orion"]["RefOperatorSchedule"] = "invalid_operationSchedule:id"
        with self.assertRaises(TypeError):
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

        self.oee.operatorSchedule["orion"]["OperatorWorkingScheduleStopsAt"][
            "value"
        ] = "3 o'clock"
        with self.assertRaises(ValueError):
            self.oee.get_todays_shift_limits()

        del self.oee.operatorSchedule["orion"]["OperatorWorkingScheduleStopsAt"][
            "value"
        ]
        with self.assertRaises(KeyError):
            self.oee.get_todays_shift_limits()

        self.oee.operatorSchedule["orion"][
            "OperatorWorkingScheduleStopsAt"
        ] = "no_value_field"
        with self.assertRaises(TypeError):
            self.oee.get_todays_shift_limits()

    def test_get_job_id(self):
        self.oee.ws["orion"] = copy.deepcopy(self.jsons["Workstation"])
        self.assertEqual(self.oee.get_job_id(), "urn:ngsi_ld:Job:202200045")
        self.oee.ws["orion"]["RefJob"] = None
        with self.assertRaises(TypeError):
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

        del self.oee.job["orion"]["CurrentOperationType"]["value"]
        with self.assertRaises(KeyError):
            self.oee.get_operation()

        self.oee.job["orion"]["CurrentOperationType"] = None
        with self.assertRaises(TypeError):
            self.oee.get_operation()

    def test_get_objects_shift_limits(self):
        self.oee.now = datetime(2022, 8, 23, 13, 0, 0)
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
        self.oee.operatorSchedule["orion"] = copy.deepcopy(
            self.jsons["OperatorSchedule"]
        )
        self.oee.get_todays_shift_limits()
        self.assertEqual(
            self.oee.get_query_start_timestamp(how="from_midnight"),
            self.oee.datetimeToMilliseconds(datetime(2022, 4, 5, 0, 0, 0)),
        )
        self.assertEqual(
            self.oee.get_query_start_timestamp(how="from_schedule_start"),
            self.oee.datetimeToMilliseconds(datetime(2022, 4, 5, 8, 0, 0)),
        )

        with self.assertRaises(NotImplementedError):
            self.oee.get_query_start_timestamp(how="somehow_else")

    def test_query_todays_data(self):
        self.oee.now = datetime(2022, 4, 4, 13, 0, 0)
        self.oee.operatorSchedule["orion"] = copy.deepcopy(
            self.jsons["OperatorSchedule"]
        )
        self.oee.get_objects_shift_limits()
        self.oee.ws["df"] = self.oee.query_todays_data(
            self.con, self.oee.ws["postgres_table"], how="from_midnight"
        )
        df = self.ws_df.copy()
        df["recvtimets"] = df["recvtimets"].map(str).map(int)
        df.dropna(how="any", inplace=True)
        start_timestamp = self.oee.datetimeToMilliseconds(datetime(2022, 4, 4))
        df = df[
            (start_timestamp <= df["recvtimets"])
            & (df["recvtimets"] <= self.oee.now_unix())
        ]
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
        start_timestamp = self.oee.datetimeToMilliseconds(datetime(2022, 4, 4, 8, 0, 0))
        df = df[
            (start_timestamp <= df["recvtimets"])
            & (df["recvtimets"] <= self.oee.now_unix())
        ]
        df["recvtimets"] = df["recvtimets"].map(str)
        df.reset_index(inplace=True, drop=True)
        # self.oee.ws["df"].dropna(how="any", inplace=True)
        # df.dropna(how="any", inplace=True)
        # self.oee.ws["df"].dtypes.to_csv("oee_ws_df_dtype.csv")
        # df.dtypes.to_csv("calculated_df_dtype.csv")
        # self.oee.ws["df"].to_csv("oee_ws_df.csv")
        # df.to_csv("calculated_df.csv")
        self.assertTrue(self.oee.ws["df"].equals(df))

        with patch("pandas.read_sql_query") as mock_read_sql_query:
            mock_read_sql_query.side_effect = psycopg2.errors.UndefinedTable
            with self.assertRaises(RuntimeError):
                self.oee.ws["df"] = self.oee.query_todays_data(
                    self.con, self.oee.ws["postgres_table"], how="from_midnight"
                )

    def test_get_current_job_start_time_today(self):
        self.oee.now = datetime(2022, 4, 4, 13, 0, 0)
        self.oee.operatorSchedule["orion"] = copy.deepcopy(
            self.jsons["OperatorSchedule"]
        )
        self.oee.get_todays_shift_limits()
        self.oee.job["id"] = "urn:ngsi_ld:Job:202200045"
        self.oee.ws["orion"] = copy.deepcopy(self.jsons["Workstation"])
        self.oee.ws["postgres_table"] = self.oee.get_cygnus_postgres_table(
            self.oee.ws["orion"]
        )
        ws_df = self.oee.query_todays_data(
            self.con, self.oee.ws["postgres_table"], how="from_midnight"
        )
        ws_df["recvtimets"] = ws_df["recvtimets"].map(str).map(float).map(int)
        self.oee.ws["df"] = ws_df.copy()
        # self.oee.convertRecvtimetsToInt(self.oee.ws["df"])
        self.assertEqual(
            # the Job was not started today, return shift start time
            self.oee.get_current_job_start_time_today(),
            datetime(2022, 4, 4, 8, 0, 0),
        )

        # the current Job start time should be 9h if we insert
        # the following
        dt_at_9h = datetime(2022, 4, 4, 9, 0, 0)
        ts_at_9h = self.oee.datetimeToMilliseconds(dt_at_9h)
        ws_df.loc[len(ws_df)] = [
            int(ts_at_9h),
            self.oee.msToDateTimeString(ts_at_9h),
            "/",
            "urn:ngsi_ld:Workstation:1",
            "Workstation",
            "RefJob",
            "Text",
            "urn:ngsi_ld:Job:202200045",
            "[]",
        ]
        ws_df.sort_values(by=["recvtimets"], inplace=True)

        self.oee.ws["df"] = ws_df.copy()
        self.assertEqual(self.oee.get_current_job_start_time_today(), dt_at_9h)

        # the following should cause
        # an error because of a Job id mismatch
        dt_at_10h = datetime(2022, 4, 4, 10, 0, 0)
        ts_at_10h = self.oee.datetimeToMilliseconds(dt_at_10h)
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

    def test_set_RefStartTime(self):
        self.oee.now = datetime(2022, 4, 5, 13, 46, 40)
        self.oee.operatorSchedule["orion"] = copy.deepcopy(
            self.jsons["OperatorSchedule"]
        )
        self.oee.get_todays_shift_limits()

        with patch(
            "OEE.OEECalculator.get_current_job_start_time_today"
        ) as mock_get_start_time:
            dt_9_40 = datetime(2022, 4, 5, 9, 40, 0)
            mock_get_start_time.return_value = dt_9_40
            self.oee.set_RefStartTime()
            self.assertEqual(self.oee.today["RefStartTime"], dt_9_40)

            dt_7_40 = datetime(2022, 4, 5, 7, 40, 0)
            mock_get_start_time.return_value = dt_7_40
            self.oee.set_RefStartTime()
            self.assertEqual(
                self.oee.today["RefStartTime"],
                self.oee.today["OperatorWorkingScheduleStartsAt"],
            )

    def test_convert_dataframe_to_str(self):
        ws_df = self.ws_df.copy()
        ws_df["recvtimets"] = ws_df["recvtimets"].map(str)
        str_ws_df = ws_df.copy()
        ws_df["recvtimets"] = ws_df["recvtimets"].map(float).map(int)
        self.assertTrue(self.oee.convert_dataframe_to_str(ws_df).equals(str_ws_df))

    def test_sort_df_by_time(self):
        ws_df = self.ws_df.copy()
        ws_df["recvtimets"] = ws_df["recvtimets"].map(str).map(float).map(int)
        dt_at_9h = datetime(2022, 4, 4, 9, 0, 0)
        ts_at_9h = self.oee.datetimeToMilliseconds(dt_at_9h)
        # append an entry, thus intentionally spoiling the timewise order
        ws_df.loc[len(ws_df)] = [
            int(ts_at_9h),
            self.oee.msToDateTimeString(ts_at_9h),
            "/",
            "urn:ngsi_ld:Workstation:1",
            "Workstation",
            "RefJob",
            "Text",
            "urn:ngsi_ld:Job:202200045",
            "[]",
        ]
        self.oee.ws["df"] = ws_df.copy()
        ws_df.sort_values(by=["recvtimets"], inplace=True)
        self.oee.ws["df"] = self.oee.sort_df_by_time(self.oee.ws["df"])
        self.assertTrue(self.oee.ws["df"].equals(ws_df))
        self.oee.ws["df"]["recvtimets"] = self.oee.ws["df"]["recvtimets"].map(str)
        with self.assertRaises(ValueError):
            self.oee.sort_df_by_time(self.oee.ws["df"])

    """
    datetime.datetime cannot be patched directly,
    patch datetime inside module
    """

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_prepare(self, mock_datetime):
        now = datetime(2022, 4, 5, 9, 0, 0)
        mock_datetime.now.return_value = now
        midnight = datetime(2022, 4, 5, 0, 0, 0)
        _8h = datetime(2022, 4, 5, 8, 0, 0)
        _16h = datetime(2022, 4, 5, 16, 0, 0)
        self.oee.prepare(self.con)
        self.assertEqual(self.oee.now_unix(), self.oee.datetimeToMilliseconds(now))
        self.assertEqual(
            remove_orion_metadata(self.oee.ws["orion"]), self.jsons["Workstation"]
        )
        self.assertEqual(
            remove_orion_metadata(self.oee.operatorSchedule["orion"]),
            self.jsons["OperatorSchedule"],
        )
        self.assertEqual(self.oee.today["OperatorWorkingScheduleStartsAt"], _8h)
        self.assertEqual(self.oee.today["OperatorWorkingScheduleStopsAt"], _16h)
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

        ws_df = self.prepare_df_between(self.ws_df.copy(), midnight, now)
        self.assertTrue(self.oee.ws["df"].equals(ws_df))

        job_df = self.prepare_df_between(self.job_df.copy(), _8h, now)
        # self.write_df_with_dtypes(job_df.sort_values(by=["recvtimets", "attrname"]), "job_calc")
        # self.write_df_with_dtypes(self.oee.job["df"].sort_values(by=["recvtimets", "attrname"]), "job_oee")
        self.assertTrue(self.are_dfs_equal(self.oee.job["df"], job_df))

        self.assertEqual(
            self.oee.oee["id"], self.jsons["Workstation"]["RefOEE"]["value"]
        )
        self.assertEqual(
            self.oee.oee["RefWorkstation"]["value"], self.jsons["Workstation"]["id"]
        )
        self.assertEqual(
            self.oee.oee["RefJob"]["value"], self.jsons["Job202200045"]["id"]
        )
        self.assertEqual(self.oee.today["RefStartTime"], _8h)

        # with patch("datetime.datetime.now") as mock_now:
        # mock_now.return_value = now
        with patch("OEE.OEECalculator.get_objects_shift_limits") as mock_get_objects:
            mock_get_objects.side_effect = RuntimeError
            with self.assertRaises(RuntimeError):
                self.oee.prepare(self.con)
            mock_get_objects.side_effect = KeyError
            with self.assertRaises(KeyError):
                self.oee.prepare(self.con)
            mock_get_objects.side_effect = AttributeError
            with self.assertRaises(AttributeError):
                self.oee.prepare(self.con)
            mock_get_objects.side_effect = TypeError
            with self.assertRaises(TypeError):
                self.oee.prepare(self.con)
        with patch("OEE.OEECalculator.is_datetime_in_todays_shift") as mock_is_in_shift:
            mock_is_in_shift.return_value = False
            with self.assertRaises(ValueError):
                self.oee.prepare(self.con)

    def test_filter_in_relation_to_RefStartTime(self):
        ms = 1649053800000
        self.oee.today["RefStartTime"] = self.oee.msToDateTime(ms)
        _8h = datetime(2022, 4, 4, 8, 0, 0)
        # _8h40 = datetime(2022, 4, 4, 8, 40, 0)
        _9h = datetime(2022, 4, 4, 9, 0, 0)
        # self.oee.now = _9h
        # self.oee.today["RefStartTime"] = _8h40
        df = self.prepare_df_between(self.ws_df, _8h, _9h)
        # df["recvtimets"] = df["recvtimets"].map(str).map(float).map(int)
        # df.sort_values(by=["recvtimets"], inplace=True)
        df_after = df[ms <= df["recvtimets"]]
        df_after.dropna(how="any", inplace=True)
        df_after.reset_index(drop=True, inplace=True)
        df_filt = self.oee.filter_in_relation_to_RefStartTime(df, how="after")
        self.logger.debug(f"df_filt: {df_filt}")
        self.logger.debug(f"df_after: {df_after}")
        self.assertTrue(df_filt.equals(df_after))
        df_before = df[ms > df["recvtimets"]]
        df_before.dropna(how="any", inplace=True)
        df_before.reset_index(drop=True, inplace=True)
        self.assertTrue(
            self.oee.filter_in_relation_to_RefStartTime(df, how="before").equals(
                df_before
            )
        )
        with self.assertRaises(NotImplementedError):
            self.oee.filter_in_relation_to_RefStartTime(df, how="somehow_else")

    def test_calc_availability_if_no_availability_record_after_RefStartTime(self):
        _8h = datetime(2022, 4, 4, 8, 0, 0)
        _8h40 = datetime(2022, 4, 4, 8, 40, 0)
        _9h = datetime(2022, 4, 4, 9, 0, 0)
        self.oee.now = _9h
        self.oee.today["RefStartTime"] = _8h40
        df = self.prepare_df_between(self.ws_df, _8h, _8h40)
        # the RefStartTime is 8:40, the last entry (as of 9h) is 8:30 turn on, so availability = 1
        self.assertEqual(
            self.oee.calc_availability_if_no_availability_record_after_RefStartTime(df),
            1,
        )
        total_time_so_far_in_shift = self.oee.datetimeToMilliseconds(
            _9h
        ) - self.oee.datetimeToMilliseconds(_8h40)
        self.assertEqual(
            self.oee.total_time_so_far_in_shift, total_time_so_far_in_shift
        )
        self.assertEqual(self.oee.total_available_time, total_time_so_far_in_shift)
        # the RefStartTime is 8:40, the last entry (as of 9h) is 8:30 turn off, so availability = 0
        df.at[-1, "attrvalue"] = "false"
        self.assertEqual(
            self.oee.calc_availability_if_no_availability_record_after_RefStartTime(df),
            0,
        )
        self.assertEqual(
            self.oee.total_time_so_far_in_shift, total_time_so_far_in_shift
        )
        self.assertEqual(self.oee.total_available_time, 0)
        df.at[-1, "attrvalue"] = "False"
        with self.assertRaises(ValueError):
            self.oee.calc_availability_if_no_availability_record_after_RefStartTime(df)

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_calc_availability_if_exists_record_after_RefStartTime(self, mock_datetime):
        now = datetime(2022, 4, 4, 9, 0, 0)
        mock_datetime.now.return_value = now
        self.oee.prepare(self.con)
        df = self.oee.ws["df"].copy()
        df_av = df[df["attrname"] == "Available"]
        df_after = self.oee.filter_in_relation_to_RefStartTime(df_av, how="after")
        # self.logger.debug("calc_availability_if_exists_record_after_RefStartTime, df_av: {df_av}")
        self.assertEqual(
            self.oee.calc_availability_if_exists_record_after_RefStartTime(df_after),
            50 / 60,
        )

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_calc_availability(self, mock_datetime):
        now = datetime(2022, 4, 4, 9, 0, 0)
        mock_datetime.now.return_value = now
        self.oee.prepare(self.con)
        df = self.oee.ws["df"].copy()
        df_av = df[df["attrname"] == "Available"]
        self.assertEqual(self.oee.calc_availability(df_av), 50 / 60)

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_handle_availability(self, mock_datetime):
        now = datetime(2022, 4, 4, 9, 0, 0)
        mock_datetime.now.return_value = now
        self.oee.prepare(self.con)
        self.oee.handle_availability()
        self.assertEqual(self.oee.oee["Availability"]["value"], 50 / 60)
        # empty df
        self.oee.ws["df"] = self.oee.ws["df"].drop(self.oee.ws["df"].index)
        with self.assertRaises(ValueError):
            self.oee.handle_availability()

    def test_count_nonzero_unique(self):
        uniques = np.array(["1", "2", "3", "6"])
        self.assertEqual(self.oee.count_nonzero_unique(uniques), 4)
        uniques = np.array(["1", "2", "0", "3", "6"])
        self.assertEqual(self.oee.count_nonzero_unique(uniques), 4)

    def test_count_injection_mouldings(self):
        now = datetime(2022, 4, 4, 9, 0, 0)
        _8h = datetime(2022, 4, 4, 8, 0, 0)
        job_df = self.prepare_df_between(self.job_df.copy(), _8h, now)
        self.oee.job["df"] = job_df
        self.oee.count_injection_mouldings()
        n_successful_mouldings = 70
        n_failed_mouldings = 1
        n_total_mouldings = n_successful_mouldings + n_failed_mouldings
        self.assertEqual(self.oee.n_successful_mouldings, n_successful_mouldings)
        self.assertEqual(self.oee.n_failed_mouldings, n_failed_mouldings)
        self.assertEqual(self.oee.n_total_mouldings, n_total_mouldings)

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_handle_quality(self, mock_datetime):
        now = datetime(2022, 4, 4, 9, 0, 0)
        mock_datetime.now.return_value = now
        self.oee.prepare(self.con)
        self.oee.handle_quality()
        n_successful_mouldings = 70
        n_failed_mouldings = 1
        n_total_mouldings = n_successful_mouldings + n_failed_mouldings
        self.assertEqual(
            self.oee.oee["Quality"]["value"], n_successful_mouldings / n_total_mouldings
        )
        self.oee.job["df"] = self.oee.job["df"].drop(self.oee.job["df"].index)
        with self.assertRaises(ValueError):
            self.oee.handle_quality()

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_handle_performance(self, mock_datetime):
        now = datetime(2022, 4, 4, 9, 0, 0)
        mock_datetime.now.return_value = now
        self.oee.prepare(self.con)
        self.oee.handle_availability()
        self.oee.handle_quality()
        self.oee.handle_performance()
        n_successful_mouldings = 70
        n_failed_mouldings = 1
        n_total_mouldings = n_successful_mouldings + n_failed_mouldings
        """
        This performance value is higher than 1 because
        for testing purposes, the 8:20 to 8:30 (GMT+2) time interval was set as unavailable
        But as this modification was made afterwards, the Job log shows
        production during this period of 10 minutes
        So the total available time is 50 minutes, but 60 minutes worth of production
        log is present in the job logs, this is why the performance is so high
        """
        performance = (n_total_mouldings * 46) / (50 * 60)
        self.assertEqual(self.oee.oee["Performance"]["value"], performance)
        # self.oee['Performance']['value'] = self.n_total_mouldings * self.operation['orion']['OperationTime']['value'] / self.total_available_time

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_calculate_OEE(self, mock_datetime):
        now = datetime(2022, 4, 4, 9, 0, 0)
        mock_datetime.now.return_value = now
        self.oee.prepare(self.con)
        oeeCalculator_oee = self.oee.calculate_OEE()
        oee = copy.deepcopy(self.jsons["OEE"])
        oee["Availability"]["value"] = 50 / 60
        oee["Quality"]["value"] = 70 / 71
        oee["Performance"]["value"] = (71 * 46) / (50 * 60)
        oee["OEE"]["value"] = (
            oee["Availability"]["value"]
            * oee["Quality"]["value"]
            * oee["Performance"]["value"]
        )
        self.assertAlmostEqual(
            self.oee.oee["Availability"]["value"],
            oee["Availability"]["value"],
            places=PLACES,
        )
        self.assertAlmostEqual(
            self.oee.oee["Quality"]["value"], oee["Quality"]["value"], places=PLACES
        )
        self.assertAlmostEqual(
            self.oee.oee["Performance"]["value"],
            oee["Performance"]["value"],
            places=PLACES,
        )
        self.assertAlmostEqual(
            self.oee.oee["OEE"]["value"], oee["OEE"]["value"], places=PLACES
        )
        self.assertAlmostEqual(
            oeeCalculator_oee["Availability"]["value"],
            oee["Availability"]["value"],
            places=PLACES,
        )
        self.assertAlmostEqual(
            oeeCalculator_oee["Quality"]["value"],
            oee["Quality"]["value"],
            places=PLACES,
        )
        self.assertAlmostEqual(
            oeeCalculator_oee["Performance"]["value"],
            oee["Performance"]["value"],
            places=PLACES,
        )
        self.assertAlmostEqual(
            oeeCalculator_oee["OEE"]["value"], oee["OEE"]["value"], places=PLACES
        )

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_calculate_throughput(self, mock_datetime):
        now = datetime(2022, 4, 4, 9, 0, 0)
        mock_datetime.now.return_value = now
        self.oee.prepare(self.con)
        self.oee.calculate_OEE()
        oeeCalculator_throughput = self.oee.calculate_throughput()
        throughput = (8 * 3600e3 / 46e3) * 8 * self.oee.oee["OEE"]["value"]
        self.assertAlmostEqual(
            oeeCalculator_throughput["ThroughputPerShift"]["value"],
            throughput,
            places=PLACES,
        )


def main():
    unittest.main()


if __name__ == "__main__":
    main()
