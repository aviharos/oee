# Standard Library imports
import datetime
import json
import os
import sys
import unittest

# PyPI imports
import pandas as pd
import numpy as np
from sqlalchemy.types import DateTime, Float, BigInteger, Text

# Custom imports
sys.path.insert(0, os.path.join('..', 'app')

# Constants
WS_FILE = 'urn_ngsi_ld_job_202200045_job.csv'
WS_TABLE = 'urn_ngsi_ld_job_202200045_job'
JOB_FILE = 'urn_ngsi_ld_workstation_1_workstation.csv'
JOB_TABLE = 'urn_ngsi_ld_workstation_1_workstation'
PLACES = 4

from conf import conf
from OEE import OEE
import upload_jsons_to_Orion

class testOrion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        upload_jsons_to_Orion.main()
        self.oee = OEE('urn:ngsi_ld:Workstation:1')
        self.oee.ws['df'] = pd.read_csv(os.path.join('csv', 'urn_ngsi_ld_workstation_1_workstation.csv'))
        global engine = create_engine(f'postgresql://{conf["postgresUser"]}:{conf["postgresPassword"]}@{conf["postgresHost"]}:{conf["postgresPort"]}')
        global con = engine.connect()

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        oee = self.oee.copy() 

    def tearDown(self):
        pass

    def test_msToDateTimeString(self):
        self.assertEqual('2022-04-05 13:46:40.000', oee.msToDateTimeString(1649159200))

    def test_stringToDateTime(self, string):
        self.assertEqual(datetime.datetime(2022, 04, 05, 13, 46, 40), oee.stringToDateTime('2022-04-05 13:46:40.000'))
    
    def test_timeToDatetime(self):
        self.assertEqual(datetime.datetime(2022, 04, 05, 13, 46, 40), oee.timeToDatetime('13:46:40.000'))

    def test_datetimeToMilliseconds(self):
        self.assertEqual(datetime.datetime(2022, 04, 05, 13, 46, 40), oee.datetimeToMilliseconds(1649159200))

    def test_convertRecvtimetsToInt(self):
        oee.convertRecvtimetsToInt(oee.ws['df']['recvtimets'])
        self.assertEqual(oee.ws['df']['recvtimets'].dtype == np.int64)

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
        op = {
                "type": "Operation",
                "OperationNumber": {"type": "Number", "value": 10},
                "OperationTime": {"type": "Number", "value": 46},
                "OperationType": {"type": "Text", "value": "Core001_injection_moulding"},
                "PartsPerOperation": {"type": "Number", "value": 8}
            }
        self.assertEqual(oee.get_operation(part, 'Core001_injection_moulding'), op)
        with self.assertRaises(AttributeError):
            oee.get_operation(part, 'Core001_painting')

    def test_updateObjects(self):
        oee.updateObjects()
        operator_schedule = json.load(os.path.join('..', 'json', 'Operatorschedule.json'))
        ws = json.load(os.path.join('..', 'json', 'Workstation.json'))
        job = json.load(os.path.join('..', 'json', 'Job202200045.json'))
        part = json.load(os.path.join('..', 'json', 'Core001.json'))
        self.assertEqual(oee.operator_schedule, operator_schedule)
        self.assertEqual(oee.ws, ws)
        self.assertEqual(oee.job, job)
        self.assertEqual(oee.part, part)
        self.assertEqual(oee.job['postgres_table'], 'urn_ngsi_ld_Job_202200045_job')

    def test_checkTime(self):
        oee.now = datetime.today() + datetime.timedelta(hours = 3)
        self.assertEqual(oee.checkTime(), True)
        oee.now = datetime.today() + datetime.timedelta(hours = 3)
        self.assertEqual(oee.checkTime(), False)
        oee.now = datetime.today() + datetime.timedelta(hours = 23)
        self.assertEqual(oee.checkTime(), False)

    def test_areConditionsOK(self):
        pass

    def test_prepare(self):
        now = datetime.now()
        today = now.date()
        startOfToday = oee.test_stringToDateTime(str(today) + ' 00:00:00.000')
        oee.prepare()
        self.assertAlmostEqual(now.unix(), oee.now.unix())
        self.assertAlmostEqual(today.unix(), oee.today['day'].unix())
        self.assertAlmostEqual(startOfToday.unix(), oee.today['start'].unix())

    def test_download_ws_df(self):
        self.ws_df = pd.read_csv(os.path.join('csv', WS_FILE))
        ws_df = self.ws_df
        ws_df.to_sql(name=WS_TABLE, con=con, schema=conf['postgresSchema'], index=False, dtype=Text, if_exists='replace')
        oee.download_ws_df()
        self.assertTrue(oee.ws['df'].equals(ws_df))

    def test_calc_availability(self):
        # Human readable timestampts are in UTC+0200 CEST
        # the machine starts up at 6:50 in the morning in the test data
        # this why the availability is high
        oee.ws['df'] = ws_df
        oee.now_unix = 1649081400000
        oee.now = datetime.datetime(2022, 4, 4, 16, 10, 0)
        availability = oee.calc_availability()
        self.assertAlmostEqual(availability, 1.119823, places=PLACES)
        oee.now_unix = 1649059200000
        oee.now = datetime.datetime(2022, 4, 4, 10, 0, 0)
        availability = oee.calc_availability()
        self.assertAlmostEqual(availability, 1.579306, places=PLACES)

    def test_handleAvailability(self):
        oee.now_unix = 1649059200000
        oee.now = datetime.datetime(2022, 4, 4, 10, 0, 0)
        oee.handleAvailability()
        self.assertEqual(oee.ws['df']['recvtimets'].dtype, np.int64)
        self.assertAlmostEqual(oee.oee['availability'], 1.119823, places=PLACES)

        oee.now_unix = 1649081400000
        oee.now = datetime.datetime(2022, 4, 4, 16, 10, 0)
        oee.handleAvailability()
        self.assertEqual(oee.ws['df']['recvtimets'].dtype, np.int64)
        self.assertAlmostEqual(oee.oee['availability'], 1.579306, places=PLACES)

        ws_df = self.ws_df.copy()
        # drop all rows
        ws_df = ws_df[False]
        ws_df.to_sql(name=WS_TABLE, con=con, schema=conf['postgresSchema'], index=False, dtype=Text, if_exists='replace')
        with self.assertRaises(ValueError):
            oee.handleAvailability()

    def test_download_job_df(self):
        pass
        # try:
        #     self.job['df'] = pd.read_sql_query(f'''select * from {conf['postgresSchema']}.{self.job["postgres_table"]}
        #                                         where {self.datetimeToMilliseconds(self.today['start'])} < cast (recvtimets as bigint)
        #                                         and cast (recvtimets as bigint) <= {self.now_unix};''', con=con)
        # except (psycopg2.errors.UndefinedTable,
        #         sqlalchemy.exc.ProgrammingError) as error:
        #     raise RuntimeError(f'The SQL table: {self.job["postgres_table"]} does not exist within the schema: {conf["postgresSchema"]}. Traceback:\n{error}') from error

    def test_countMouldings(self):
        pass
        # df = self.job['df']
        # self.n_successful_mouldings = len(df[df.attrname == 'GoodPartCounter']['attrvalue'].unique())
        # self.n_failed_mouldings = len(df[df.attrname == 'RejectPartCounter']['attrvalue'].unique())
        # self.n_total_mouldings = self.n_successful_mouldings + self.n_failed_mouldings

    def test_handleQuality(self):
        pass
        # self.download_job_df(con)
        # self.convertRecvtimetsToInt(self.job['df']['recvtimets'])
        # # self.job['df']['recvtimets'] = self.job['df']['recvtimets'].map(float).map(int)
        # if self.job['df'].size == 0:
        #     raise ValueError(f'No job data found for {self.job["id"]} up to time {self.now} on day {self.today}.')
        # self.countMouldings()
        # if self.n_total_mouldings == 0:
        #     raise ValueError('No operation was completed yet, no OEE data')
        # self.oee['quality'] = self.n_successful_mouldings / self.n_total_mouldings
        
    def test_handlePerformance(self):
        pass
        # status_code, job_json = Orion.getObject(self.job['id'])
        # if status_code != 200:
        #     raise RuntimeError(f'Failed to get object from Orion broker:{self.job["id"]}, status_code:{status_code}; no OEE data')
        # try:
        #     partId = job_json['RefPart']['value']
        # except (KeyError, TypeError) as error:
        #     raise RuntimeError('Critical: RefPart not found in the Job {self.job["id"]}: {job_json}') from error
        # status_code, part_json = Orion.getObject(partId)
        # if status_code != 200:
        #     raise RuntimeError(f'Failed to get object from Orion broker:{partId}, status_code:{status_code}; no OEE data')
        # try:
        #     current_operation_type = job_json['CurrentOperationType']['value']
        # except (KeyError, TypeError) as error:
        #     raise RuntimeError(f'Critical: CurrentOperationType not found in the Job {self.job["id"]}: {job_json}') from error
        # try:
        #     operation = self.get_operation(part_json, current_operation_type)
        # except (KeyError, TypeError) as error:
        #     raise RuntimeError(f'Critical: Operation {current_operation_type} not found in the Part: {part_json}') from error
        # try:
        #     self.operationTime = operation['OperationTime']['value']
        # except (KeyError, TypeError) as error:
        #     raise RuntimeError(f'Critical: OperationTime not found in the Part: {part_json}') from error
        # try:
        #     self.partsPerOperation = operation['PartsPerOperation']['value']
        # except (KeyError, TypeError) as error:
        #     raise RuntimeError(f'Critical: partsPerOperation not found in the Part: {part_json}') from error
        # self.oee['performance'] = self.n_total_mouldings * self.operationTime / self.total_available_time

    def test_calculateOEE(self):
        pass
        # self.handleAvailability(con)
        # self.handleQuality(con)
        # self.handlePerformance()
        # self.oee['oee'] = self.oee['availability'] * self.oee['performance'] * self.oee['quality']
        # self.jobIds = self.job['id']
        # self.logger.info(f'oee: {self.oee}, jobIds: {self.job["id"]}')
        # return self.oee, self.job['id']

    def test_calculateThroughput(self):
        pass
        # shiftLengthInMilliseconds = self.datetimeToMilliseconds(self.today['OperatorScheduleStopsAt']) - self.datetimeToMilliseconds(self.today['OperatorScheduleStartsAt'])
        # self.throughput = (shiftLengthInMilliseconds / self.operationTime) * self.partsPerOperation * self.oee['oee']
        # self.logger.info(f'Throughput: {self.throughput}')
        # return self.throughput

    def insert(self, con):
        pass
        # table_name = self.ws['id'].replace(':', '_').lower() + '_workstation_oee'
        # oeeData = pd.DataFrame.from_dict({'recvtimets': [self.now_unix],
        #                                   'recvtime': [self.msToDateTimeString(self.now_unix)],
        #                                   'availability': [self.oee['availability']],
        #                                   'performance': [self.oee['performance']],
        #                                   'quality': [self.oee['quality']],
        #                                   'oee': [self.oee['oee']],
        #                                   'throughput_shift': [self.throughput],
        #                                   'jobs': [self.jobIds]})
        # oeeData.to_sql(name=table_name, con=con, schema=conf['postgresSchema'], index=False, dtype=self.col_dtypes, if_exists='append')
        # self.logger.debug('Successfully inserted OEE data into Postgres')

if __name__ == '__main__':
    ans = input('''The testing process needs MOMAMS up and running.
Please start it if you have not already.
Also, the tests delete and create objects in the Orion broker.
It also changes the PostgreSQL data.
Never use the tests on a production environment.
Do you still want to proceed? [yN]''')
    if ans != 'y':
        print('exiting...')
        sys.exit(0)
    try:
        unittest.main()
    except error:
        print(error)
    finally:
        con.close()
        engine.dispose()

