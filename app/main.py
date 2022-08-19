# -*- coding: utf-8 -*-
"""
Install dependencies in Miniconda environment (just once)
conda install python=3.8 spyder sqlalchemy pandas psycopg2
"""

# Standard Library imports
import json
import os
import sched
import time

# PyPI packages
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2

# Custom imports, config
from conf import conf
from Logger import getLogger
from object_to_template import object_to_template
from OEE import OEECalculator
import Orion

logger_main = getLogger(__name__)

def handle_ws(con, ws):
    ws_id = ws['id']
    ref_job_id = ws['RefJob']['value']
    if not Orion.exists(ref_job_id):
        raise ValueError('Critical: object does not exist in Orion: {id}')
    ref_OEE_id = ws['RefOEE']['value']
    ref_Throughput_id = ws['RefThroughput']['value']
    oeeCalculator = OEECalculator(ws_id)
    oeeCalculator.prepare(con)
    oee = oeeCalculator.calculate_OEE()
    throughput = oeeCalculator.calculate_throughput()
    Orion.update((oee, throughput))
    return ws_id, ref_job_id, ref_OEE_id, ref_Throughput_id

def clear_KPI_objects(ws_id, ref_job_id, ref_OEE_id, ref_Throughput_id):
    try:
        oee = object_to_template(os.path.join('..', 'json', 'OEE.json'))
        throughput = object_to_template(os.path.join('..', 'json', 'Throughput.json'))
    except FileNotFoundError as error:
        logger_main.critical(f'OEE.json or Throughput.json not found.\n{error}')
    except json.decoder.JSONDecodeError as error:
        logger_main.critical(f'OEE.json or Throughput.json is invalid.\n{error}')
    else:
        oee['id'] = ref_OEE_id
        throughput['id'] = ref_Throughput_id
        for object_ in (oee, throughput):
            object_['RefWorkstation']['value'] = ws_id
            object_['RefJob']['value'] = ref_job_id
        Orion.update((oee, throughput))

def loop(scheduler_):
    try:
        engine = create_engine(f'postgresql://{conf["postgresUser"]}:{conf["postgresPassword"]}@{conf["postgresHost"]}:{conf["postgresPort"]}')
        con = engine.connect()
        workstations = Orion.getWorkstations()
        if len(workstations) == 0:
            logger_main.critical(f'No Workstation is found in the Orion broker, no OEE data')
        for ws in workstations:
            ws_id, ref_job_id, ref_OEE_id, ref_Throughput_id = handle_ws(con, ws)
    except (AttributeError,
            KeyError,
            RuntimeError,
            ValueError,
            ZeroDivisionError,
            psycopg2.OperationalError,
            sqlalchemy.exc.OperationalError) as error:
        logger_main.error(error)
        # could not calculate OEE or Throughput
        # try to delete the OEE and Throughput values, if we have enough data
        if ('ws_id' in locals() and
            'ref_job_id' in locals() and
            'ref_OEE_id' in locals() and
            'ref_Throughput_id' in locals()):
            clear_KPI_objects(ws_id, ref_job_id, ref_OEE_id, ref_Throughput_id)
        else:
            logger_main.critical(f'A critical error occured, not even the references of the objects could be determined. No OEE data. An OEE and Throughput object should be cleared, but it cannot be determined, which ones.')
    finally:
        try:
            con.close()
            engine.dispose()
        except NameError:
            pass
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
