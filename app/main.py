# -*- coding: utf-8 -*-
"""
Install dependencies in Miniconda environment (just once)
conda install python=3.8 spyder sqlalchemy pandas psycopg2
"""

# Standard Library imports
import sched
import time

# PyPI packages
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2

# Custom imports, config
from conf import conf
from Logger import getLogger
import OEE
import Orion

logger_main = getLogger(__name__)


# TODO remove before production
if conf['test_mode']:
    from datetime import datetime
    conf['period_time'] = 1
    DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
    time_override = datetime.strptime('2022-04-05 13:38:27.87', DATETIME_FORMAT)
else:
    time_override = None


def stringToDateTime(string):
    return datetime.strptime(string, DATETIME_FORMAT)


def loop(scheduler_):
    try:
        engine = create_engine(f'postgresql://{conf["postgresUser"]}:{conf["postgresPassword"]}@{conf["postgresHost"]}:{conf["postgresPort"]}')
        con = engine.connect()
    
        status_code_ws, workstationIds = Orion.getWorkstationIds()
        if status_code_ws == 200:
            if len(workstationIds) == 0:
                logger_main.critical(f'No Workstation is found in the Orion broker, no OEE data')
            for workstationId in workstationIds:
                status_code_job, jobId = Orion.getActiveJobId(workstationId)
                if status_code_job == 200 and jobId is not None:
                    # availability, performance, quality, oee, throughput
                    oeeData = OEE.calculateOEE(workstationId, jobId, con, _time_override=time_override)
                    if oeeData is not None:
                        (availability, performance, quality, oee, throughput) = oeeData
                        OEE.insertOEE(workstationId,
                                      availability,
                                      performance,
                                      quality,
                                      oee,
                                      throughput,
                                      jobId,
                                      con)
        con.close()
        engine.dispose()
    except (psycopg2.OperationalError,
            sqlalchemy.exc.OperationalError) as error:
        logger_main.error(f'Failed to connect to PostgreSQL. Traceback:\n{error}')
    finally:
        scheduler_.enter(conf['period_time'], 1, loop, (scheduler_,))


def main():
    scheduler = sched.scheduler(time.time, time.sleep)
    scheduler.enter(conf['period_time'], 1, loop, (scheduler,))
    logger_main.info('Starting OEE app...')
    try:
        scheduler.run()
    except KeyboardInterrupt:
        logger_main.info('KeyboardInterrupt. Stopping OEE app...')


if __name__ == '__main__':
    main()
