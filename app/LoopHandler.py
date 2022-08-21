# -*- coding: utf-8 -*-
# Standard Library imports
import json
import os

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

class LoopHandler():
    logger = getLogger(__name__)

    def __init__(self):
        self.ids = {'ws': None,
                    'job': None,
                    'oee': None,
                    'throughput': None}

    def handle(self):
        try:
            self.engine = create_engine(f'postgresql://{conf["postgresUser"]}:{conf["postgresPassword"]}@{conf["postgresHost"]}:{conf["postgresPort"]}')
            self.con = self.engine.connect()
            self.workstations = Orion.getWorkstations()
            if len(self.workstations) == 0:
                self.logger.critical(f'No Workstation is found in the Orion broker, no OEE data')
            for ws in self.workstations:
                self.ids['ws'] = ws['id']
                self.ids['job'] = ws['RefJob']['value']
                if not Orion.exists(self.ids['job']):
                    raise ValueError('Critical: object does not exist in Orion: {self.ids["job"]}')
                self.ids['oee'] = ws['RefOEE']['value']
                self.ids['throughput'] = ws['RefThroughput']['value']
                oeeCalculator = OEECalculator(self.ids['ws'])
                oeeCalculator.prepare(self.con)
                oee = oeeCalculator.calculate_OEE()
                throughput = oeeCalculator.calculate_throughput()
                Orion.update((oee, throughput))

        except (AttributeError,
                KeyError,
                RuntimeError,
                ValueError,
                ZeroDivisionError,
                psycopg2.OperationalError,
                sqlalchemy.exc.OperationalError) as error:
            self.logger.error(error)
            # could not calculate OEE or Throughput
            # try to delete the OEE and Throughput values, if we have enough data
            if not None in self.ids.values():
                try:
                    oee = object_to_template(os.path.join('..', 'json', 'OEE.json'))
                    throughput = object_to_template(os.path.join('..', 'json', 'Throughput.json'))
                except FileNotFoundError as error:
                    self.logger.critical(f'OEE.json or Throughput.json not found.\n{error}')
                except json.decoder.JSONDecodeError as error:
                    self.logger.critical(f'OEE.json or Throughput.json is invalid.\n{error}')
                else:
                    oee['id'] = self.ids['oee']
                    throughput['id'] = self.ids['throughput']
                    for object_ in (oee, throughput):
                        object_['RefWorkstation']['value'] = self.ids['ws']
                        object_['RefJob']['value'] = self.ids['job']
                    Orion.update((oee, throughput))
            else:
                self.logger.critical(f'A critical error occured, not even the references of the objects could be determined. No OEE data. An OEE and Throughput object should be cleared, but it cannot be determined, which ones.')
        finally:
            try:
                self.con.close()
                self.engine.dispose()
            except NameError:
                pass

