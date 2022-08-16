# Standard Library imports
import datetime
import json
import glob
import os
from os.path import splitext
import sys
import unittest
from unittest.mock import patch

# PyPI imports
import pandas as pd
import numpy as np
from psycopg2 import create_engine
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

from conf import conf
from OEE import OEE
import upload_jsons_to_Orion

class testOrion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        upload_jsons_to_Orion.main()
        global g_oee
        g_oee = OEE(WS_ID)
        g_oee.ws['df'] = pd.read_csv(os.path.join('csv', WS_FILE))
        g_oee.job['df'] = pd.read_csv(os.path.join('csv', JOB_FILE))
        global engine
        engine = create_engine(f'postgresql://{conf["postgresUser"]}:{conf["postgresPassword"]}@{conf["postgresHost"]}:{conf["postgresPort"]}')
        global con
        con = engine.connect()
        global g_jsons
        g_jsons = {}
        jsons = glob.glob(os.path.join('..', 'json', '*.json'))
        for file in jsons:
            json_name = os.path.splitext(os.path.basename(file))[0]
            with open(file, 'r') as f:
                g_jsons[json_name] = json.load(f)

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        oee = g_oee.copy() 

    def tearDown(self):
        pass

    def test_msToDateTimeString(self):
        self.assertEqual('2022-04-05 13:46:40.000', oee.msToDateTimeString(1649159200))

    def test_stringToDateTime(self, string):
        self.assertEqual(datetime.datetime(2022, 4, 5, 13, 46, 40), oee.stringToDateTime('2022-04-05 13:46:40.000'))
    
    def test_timeToDatetime(self):
        self.assertEqual(datetime.datetime(2022, 4, 5, 13, 46, 40), oee.timeToDatetime('13:46:40.000'))

    def test_datetimeToMilliseconds(self):
        self.assertEqual(datetime.datetime(2022, 4, 5, 13, 46, 40), oee.datetimeToMilliseconds(1649159200))

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
        op = part['Operations']['value'][0]
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
        self.assertEqual(oee.checkTime(), False)
        oee.now = datetime.today() + datetime.timedelta(hours = 9)
        self.assertEqual(oee.checkTime(), True)
        oee.now = datetime.today() + datetime.timedelta(hours = 23)
        self.assertEqual(oee.checkTime(), False)

    def test_areConditionsOK(self):
        pass

    def test_prepare(self):
        now = datetime.now()
        today = now.date()
        startOfToday = oee.test_stringToDateTime(str(today) + ' 00:00:00.000')
        oee.prepare()
        self.assertAlmostEqual(now.unix(), oee.now.unix(), places=PLACES)
        self.assertAlmostEqual(today.unix(), oee.today['day'].unix(), places=PLACES)
        self.assertAlmostEqual(startOfToday.unix(), oee.today['start'].unix(), places=PLACES)

    def test_download_ws_df(self):
        self.ws_df = pd.read_csv(os.path.join('csv', WS_FILE))
        ws_df = self.ws_df
        ws_df.to_sql(name=WS_TABLE, con=con, schema=conf['postgresSchema'], index=False, dtype=Text, if_exists='replace')
        oee.ws['df'] = None
        oee.download_ws_df()
        self.assertTrue(oee.ws['df'].equals(ws_df))

    def test_calc_availability(self):
        # Human readable timestampts are in UTC+0200 CEST
        # the machine starts up at 6:50 in the morning in the test data
        # this why the availability is high
        oee.ws['df'] = ws_df
        startTime = 1649047829000
        shiftStart = 1649052000000

        oee.now = datetime.datetime(2022, 4, 4, 16, 0, 0)
        oee.now_unix = 1649080800000
        total_available_time = oee.now_unix - startTime
        availability = total_available_time / (oee.now_unix - shiftStart)
        self.assertAlmostEqual(oee.calc_availability(), availability, places=PLACES)

        oee.now = datetime.datetime(2022, 4, 4, 10, 0, 0)
        oee.now_unix = 1649059200000
        total_available_time = oee.now_unix - startTime
        availability = total_available_time / (oee.now_unix - shiftStart)
        self.assertAlmostEqual(oee.calc_availability(), availability, places=PLACES)

    def test_handleAvailability(self):
        startTime = 1649047829000
        shiftStart = 1649052000000

        oee.now_unix = 1649059200000
        oee.now = datetime.datetime(2022, 4, 4, 10, 0, 0)
        total_available_time = oee.now_unix - startTime
        availability = total_available_time / (oee.now_unix - shiftStart)
        oee.handleAvailability()
        self.assertEqual(oee.ws['df']['recvtimets'].dtype, np.int64)
        self.assertAlmostEqual(oee.oee['availability'], availability, places=PLACES)

        oee.now_unix = 1649080800000
        oee.now = datetime.datetime(2022, 4, 4, 16, 0, 0)
        total_available_time = oee.now_unix - startTime
        availability = total_available_time / (oee.now_unix - shiftStart)
        oee.handleAvailability()
        self.assertEqual(oee.ws['df']['recvtimets'].dtype, np.int64)
        self.assertAlmostEqual(oee.oee['availability'], availability, places=PLACES)

        ws_df = self.ws_df.copy()
        # drop all rows
        ws_df.drop(ws_df.index, inplace=True)
        ws_df.to_sql(name=WS_TABLE, con=con, schema=conf['postgresSchema'], index=False, dtype=Text, if_exists='replace')
        oee.ws['df'] = None
        with self.assertRaises(ValueError):
            oee.handleAvailability()

    def test_download_job_df(self):
        self.job_df = pd.read_csv(os.path.join('csv', JOB_FILE))
        job_df = self.job_df
        job_df.to_sql(name=JOB_TABLE, con=con, schema=conf['postgresSchema'], index=False, dtype=Text, if_exists='replace')
        oee.job['df'] = None
        oee.download_job_df()
        self.assertTrue(oee.job['df'].equals(job_df))

    def test_countNumberOfMouldings(self):
        unique_values = ['16', '24', '32']
        self.assertEqual(oee.countNumberOfMouldings(unique_values), len(unique_values))
        unique_values.append['0']
        self.assertEqual(oee.countNumberOfMouldings(unique_values), len(unique_values) - 1)

    def test_countMouldings(self):
        oee.job['df'] = self.job_df
        oee.countMouldings()
        n_successful_mouldings = 562
        n_failed_mouldings = 3
        n_total_mouldings = n_successful_mouldings + n_failed_mouldings
        self.assertEqual(oee.n_successful_mouldings, n_successful_mouldings)
        self.assertEqual(oee.n_failed_mouldings, n_failed_mouldings)
        self.assertEqual(oee.n_total_mouldings, n_total_mouldings)

    def test_handleQuality(self):
        job_df = self.job_df.copy()
        job_df.to_sql(name=JOB_TABLE, con=con, schema=conf['postgresSchema'], index=False, dtype=Text, if_exists='replace')
        oee.job['df'] = None
        oee.handleQuality()
        n_successful_mouldings = 562
        n_failed_mouldings = 3
        n_total_mouldings = n_successful_mouldings + n_failed_mouldings
        self.assertAlmostEqual(oee.oee['quality'], n_successful_mouldings / n_total_mouldings, places=PLACES)

        job_df = self.job_df.copy()
        job_df.drop(job_df.index, inplace=True)
        job_df.to_sql(name=JOB_TABLE, con=con, schema=conf['postgresSchema'], index=False, dtype=Text, if_exists='replace')
        oee.job['df'] = None
        with self.assertRaises(ValueError):
            oee.handleQuality()

        job_df = self.job_df.copy()
        job_df = job_df[job_df['attrname'] != 'GoodPartCounter']
        job_df = job_df[job_df['attrname'] != 'RejectPartCounter']
        job_df.to_sql(name=JOB_TABLE, con=con, schema=conf['postgresSchema'], index=False, dtype=Text, if_exists='replace')
        oee.job['df'] = None
        with self.assertRaises(ValueError):
            oee.handleQuality()

    def test_download_job(self):
        with patch('Orion.getObject') as mocked_getObject:
            oee.job_json = None
            job_json = g_jsons['Job202200045']
            mocked_getObject.return_value = 201, job_json 
            with self.assertRaises(RuntimeError):
                oee.download_job()

        with patch('Orion.getObject') as mocked_getObject:
            oee.job_json = None
            job_json = g_jsons['Job202200045']
            mocked_getObject.return_value = 200, self.job_json 
            oee.download_job()
            self.assertEquals(oee.job_json, job_json)

    def test_get_partId(self):
        oee.job_json = g_jsons['Job202200045']
        oee.get_partId()
        self.assertEqual(oee.partId, 'urn:ngsi_ld:Part:Core001')
        del(oee.job_json['RefPart']['value'])
        with self.assertRaises(RuntimeError):
            oee.get_partId()
        del(oee.job_json['RefPart'])
        with self.assertRaises(RuntimeError):
            oee.get_partId()

    def test_download_part(self):
        with patch('Orion.getObject') as mocked_getObject:
            oee.part_json = None
            part_json = g_jsons['Core001']
            mocked_getObject.return_value = 201, part_json
            with self.assertRaises(RuntimeError):
                oee.download_part()

        with patch('Orion.getObject') as mocked_getObject:
            oee.part_json = None
            part_json = g_jsons['Core001']
            mocked_getObject.return_value = 200, part_json
            oee.download_part()
            self.assertEquals(oee.part_json, part_json)

    def test_get_current_operation_type(self):
        oee.part_json = g_jsons['Core001']
        oee.get_current_operation_type()
        self.assertEqual(oee.current_operation_type, 'Core001_injection_moulding')
        del(oee.part_json['RefPart']['value'])
        with self.assertRaises(RuntimeError):
            oee.get_current_operation_type()
        del(oee.part_json['RefPart'])
        with self.assertRaises(RuntimeError):
            oee.get_current_operation_type()

    def test_download_operation(self):
        with patch('Orion.getObject') as mocked_getObject:
            oee.operation_json = None
            operation_json = g_jsons['Core001']['Operations']['value'][0]
            mocked_getObject.return_value = 201, operation_json
            with self.assertRaises(RuntimeError):
                oee.download_part()

        with patch('Orion.getObject') as mocked_getObject:
            oee.operation_json = None
            operation_json = g_jsons['Core001']['Operations']['value'][0]
            mocked_getObject.return_value = 200, operation_json
            oee.download_part()
            self.assertEquals(oee.operation_json, operation_json)

    def test_get_operation_time(self):
        oee.operation_json = g_jsons['Core001']['Operations']['value'][0]
        oee.get_operation_time()
        self.assertEqual(oee.operationTime, 46)
        del(oee.operation_json['OperationTime']['value'])
        with self.assertRaises(RuntimeError):
            oee.get_operation_time()
        del(oee.part_json['OperationTime'])
        with self.assertRaises(RuntimeError):
            oee.get_operation_time()

    def test_get_partsPerOperation(self):
        oee.operation_json = g_jsons['Core001']['Operations']['value'][0]
        oee.get_partsPerOperation()
        self.assertEqual(oee.partsPerOperation, 8)
        del(oee.operation_json['PartsPerOperation']['value'])
        with self.assertRaises(RuntimeError):
            oee.get_partsPerOperation()
        del(oee.part_json['PartsPerOperation'])
        with self.assertRaises(RuntimeError):
            oee.get_partsPerOperation()

    def test_handlePerformance(self):
        oee.now_unix = 1649080800000
        oee.now = datetime.datetime(2022, 4, 4, 16, 0, 0)
        n_successful_mouldings = 562
        n_failed_mouldings = 3
        n_total_mouldings = n_successful_mouldings + n_failed_mouldings
        operationTime = 46
        total_available_time = 32250900
        performance = (n_total_mouldings * operationTime) / total_available_time
        oee.handlePerformance()
        self.assertAlmostEqual(oee.oee['performance'], performance)

    def test_calculateOEE(self):
        startTime = 1649047829000
        shiftStart = 1649052000000

        oee.now_unix = 1649080800000
        oee.now = datetime.datetime(2022, 4, 4, 16, 0, 0)
        total_available_time = oee.now_unix - startTime
        availability = total_available_time / (oee.now_unix - shiftStart)
        n_successful_mouldings = 562
        n_failed_mouldings = 3
        n_total_mouldings = n_successful_mouldings + n_failed_mouldings
        quality = n_successful_mouldings / n_total_mouldings
        operationTime = 46
        performance = (n_total_mouldings * operationTime) / total_available_time
        oeeManual = {'availability': availability,
                'performance': performance,
                'quality': quality,
                'oee': availability * performance * quality}
        oee.calculateOEE()
        self.assertEqual(oee.oee, oeeManual)
        self.assertEqual(oee.job['id'], 'Job202200045')

    def test_calculateThroughput(self):
        oee.now_unix = 1649080800000
        oee.now = datetime.datetime(2022, 4, 4, 16, 0, 0)
        total_available_time = oee.now_unix - startTime
        availability = total_available_time / (oee.now_unix - shiftStart)
        n_successful_mouldings = 562
        n_failed_mouldings = 3
        n_total_mouldings = n_successful_mouldings + n_failed_mouldings
        quality = n_successful_mouldings / n_total_mouldings
        operationTime = 46
        performance = (n_total_mouldings * operationTime) / total_available_time
        oee_value = availability * quality * performance
        shiftLengthInMilliseconds = 8 * 3600e3
        partsPerOperation = 8
        throughput = (shiftLengthInMilliseconds / operationTime) * partsPerOperation * oee_value
        self.assertAlmostEqual(oee.calculateThrouthput, throughput, places=PLACES)

    def test_insert(self, con):
        availability = 0.83
        quality = 0.95
        performance = 0.88
        oee_value = availability * quality * performance
        now_unix = 1649080800000
        throughput = 4611.7
        oee.nox_unix = now_unix
        oee.oee = {'availability': availability,
                   'quality': quality,
                   'performance': performance,
                   'oee': oee_value}
        oee.throughput = throughput
        oee.job['id'] = JOB_ID
        df = pd.DataFrame.from_dict({'recvtimets': now_unix,
                                     'recvtime': oee.msToDateTimeString(now_unix),
                                     'availability': availability,
                                     'performance': performance,
                                     'quality': quality,
                                     'oee': oee_value,
                                     'throughput': throughput,
                                     'job': oee.job['id']})
        # delete all data by uploading empty table
        df.drop(df.index).to_sql(name=OEE_TABLE, con=con, schema=conf['postgresSchema'], index=False, dtype=COL_DTYPES, if_exists='replace')
        oee.insert(con)
        inserted_df = pd.read_sql_query(f'''select * from {conf['postgresSchema']}.{OEE_TABLE}''')
        self.assertTrue(inserted_df.equals(df))

if __name__ == '__main__':
    ans = input('''The testing process needs MOMAMS up and running on localhost.
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
    except Exception as error:
        print(error)
    finally:
        con.close()
        engine.dispose()

