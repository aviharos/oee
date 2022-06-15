# -*- coding: utf-8 -*-
"""
Install dependencies in Miniconda environment (just once)
conda install python=3.8 spyder sqlalchemy pandas psycopg2
"""

# Standard Library imports
import logging
import sched
import sys
import time

# PyPI packages
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2

# Custom imports, config
from conf import conf
import OEE
import Orion

global conf, postgresSchema

log_levels = {'DEBUG': logging.DEBUG,
              'INFO': logging.INFO,
              'WARNING': logging.WARNING,
              'ERROR': logging.ERROR,
              'CRITICAL': logging.CRITICAL}
logger_main = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
logger_main.setLevel(log_levels[conf['logging_level']])
if conf['log_to_file']:
    file_handler_main = logging.FileHandler('OEE.log')
    file_handler_main.setFormatter(formatter)
    logger_main.addHandler(file_handler_main)
if conf['log_to_stdout']:
    stream_handler_main = logging.StreamHandler(sys.stdout)
    stream_handler_main.setFormatter(formatter)
    logger_main.addHandler(stream_handler_main)

postgresSchema = conf['postgresSchema']

if conf['test_mode']:
    conf['period_time'] = 1


def loop(scheduler_):
    try:
        global engine, con
        engine = create_engine(f'postgresql://{conf["postgresUser"]}:{conf["postgresPassword"]}@{conf["postgresHost"]}:{conf["postgresPort"]}')
        con = engine.connect()
    
        workstationIds = Orion.getWorkstationIdsFromOrion()
        for workstationId in workstationIds:
            jobId = Orion.getActiveJobId(workstationId)
            # availability, performance, quality, oee, throughput
            oeeData = OEE.calculateOEE(workstationId, jobId)
            if oeeData is not None:
                (availability, performance, quality, oee, throughput) = oeeData
                OEE.insertOEE(workstationId,
                              availability,
                              performance,
                              quality,
                              oee,
                              throughput,
                              jobId)
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
