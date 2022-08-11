# Standard Library imports
import datetime
import json
import os
import sys
import unittest

# PyPI imports
import pandas as pd
import numpy as np

# Custom imports
sys.path.insert(0, os.path.join('..', 'app')

from conf import conf
from OEE import OEE
import upload_jsons_to_Orion

class testOrion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        upload_jsons_to_Orion.main()
        self.oee = OEE('urn:ngsi_ld:Workstation:1')
        self.oee.ws['df'] = pd.read_csv(os.path.join('csv', 'urn_ngsi_ld_workstation_1_workstation.csv'))

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
        pass
        # self.now = datetime.now()
        # if _time_override is not None:
        #     self.logger.warning(f'Warning: time override:  {_time_override}')
        #     self.now = _time_override
        # self.now_unix = self.now.timestamp() * 1000
        # self.today = {'day': datetime.now().date(),
        #               'start': self.stringToDateTime(str(self.now.date()) + ' 00:00:00.000')}
        # try:
        #     self.updateObjects()
        # except (RuntimeError, KeyError, AttributeError) as error:
        #     self.logger.error(f'Could not update objects from Orion. Traceback:\n{error}')
        #     raise error

    def test_download_ws_df(self):
        pass
        # try:
        #     self.ws['df'] = pd.read_sql_query(f'''select * from {conf['postgresSchema']}.{self.ws['table_name']}
        #                                        where {self.datetimeToMilliseconds(self.today['start'])} < cast (recvtimets as bigint) 
        #                                        and cast (recvtimets as bigint) <= {self.now_unix};''', con=con)
        # except (psycopg2.errors.UndefinedTable,
        #         sqlalchemy.exc.ProgrammingError) as error:
        #     raise RuntimeError(f'The SQL table: {self.ws["postgres_table"]} does not exist within the schema: {conf["postgresSchema"]}. Traceback:\n{error}') from error

    def test_calc_availability(self):
        pass
        '''
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
        # total_available_time_hours = time.strftime("%H:%M:%S", time.gmtime(total_available_time/1000))
        # return total_available_time/(now.timestamp()*1000-self.today['OperatorScheduleStartsAt'].timestamp()*1000)
        self.total_available_time = total_available_time
        total_time_so_far_in_shift = self.datetimeToMilliseconds(self.now) - self.datetimeToMilliseconds(self.today['OperatorScheduleStartsAt'])
        if total_time_so_far_in_shift == 0:
            raise ZeroDivisionError('Total time so far in the shift is 0, no OEE data')
        return total_available_time / total_time_so_far_in_shift
        '''

    def test_handleAvailability(self):
        pass
        # self.download_ws_df(con)
        # self.convertRecvtimetsToInt(self.ws['df']['recvtimets'])
        # if self.ws['df'].size == 0:
        #     raise ValueError(f'No workstation data found for {self.ws["id"]} up to time {self.now} on day {self.today["day"]}, no OEE data')
        # self.oee['availability'] = self.calc_availability()

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
Never use the tests on a production environment.
Do you still want to proceed? [yN]''')
    if ans == 'y':
        unittest.main()
    else:
        print('exiting...')

