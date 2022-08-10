import datetime
import unittest
import sys
sys.path.insert(0, '../app')

from OEE import OEE
from conf import conf

class testOrion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        oee = OEE('urn:ngsi_ld:Workstation:1')

    def tearDown(self):
        pass

    def test_msToDateTimeString(self, ms):
        self.assertEqual('2022-04-05 13:46:40.000', oee.msToDateTimeString(1649159200))

    def test_stringToDateTime(self, string):
        self.assertEqual(datetime.datetime(2022, 04, 05, 13, 46, 40), oee.stringToDateTime('2022-04-05 13:46:40.000'))
    
    def test_timeToDatetime(self, string):
        self.assertEqual(datetime.datetime(2022, 04, 05, 13, 46, 40), oee.timeToDatetime('13:46:40.000'))

    def test_datetimeToMilliseconds(self, datetime_):
        pass
        # return datetime_.timestamp()*1000

    def test_convertRecvtimetsToInt(self, col):
        pass
        # col = col.map(float).map(int)

    def test_get_operation(self, part, operationType):
        pass
        # for operation in part['Operations']['value']:
        #     if operation['OperationType']['value'] == operationType:
        #         return operation
        # raise AttributeError(f'The part {part} has no operation with type {operationType}')

    def test_updateObjects(self):
        pass
        # self.operatorSchedule = Orion.getObject('urn:ngsi_ld:OperatorSchedule:1')
        # try:
        #     for time_ in ('OperatorScheduleStartsAt', 'OperatorScheduleStopsAt'):
        #         self.today[time_] = self.timeToDatetime(self.operatorSchedule[time_]['value'])
        # except (ValueError, KeyError) as error:
        #     raise ValueError(f'Critical: could not convert time in Operatorschedule. Traceback:\n{error}')
        # self.ws = Orion.getObject(self.ws['id'])
        # self.job['orion'] = Orion.getObject(self.ws['orion']['RefJob']['value'])
        # self.job['postgres_table'] = self.job['id'].replace(':', '_').lower() + '_job'
        # self.part = Orion.getObject(self.job['orion']['RefPart']['value'])

    def test_checkTime(self):
        pass
        # if self.now < self.today['OperatorScheduleStartsAt']:
        #     self.logger.info(f'The shift has not started yet before time: {self.now}, no OEE data')
        #     return False
        # if self.now > self.today['OperatorScheduleStopsAt']:
        #     self.logger.info(f'The current time: {self.now} is after the shift\'s end, no OEE data')
        #     return False

    def test_areConditionsOK(self):
        pass
        # if not self.checkTime():
        #     return False
        # else:
        #     return True

    def test_prepare(self, _time_override=None):
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

    def test_download_ws_df(self, con):
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

    def test_handleAvailability(self, con):
        pass
        # self.download_ws_df(con)
        # self.convertRecvtimetsToInt(self.ws['df']['recvtimets'])
        # if self.ws['df'].size == 0:
        #     raise ValueError(f'No workstation data found for {self.ws["id"]} up to time {self.now} on day {self.today["day"]}, no OEE data')
        # self.oee['availability'] = self.calc_availability()

    def test_download_job_df(self, con):
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

    def test_handleQuality(self, con):
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

    def test_calculateOEE(self, con):
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

