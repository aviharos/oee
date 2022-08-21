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
    blank_ids = {'ws': None,
                 'job': None,
                 'oee': None,
                 'throughput': None}

    # def __init__(self):

    def delete_attributes(self, object_):
        file = f'{object_}.json'
        try:
            orion_object = object_to_template(os.path.join('..', 'json', file))
        except FileNotFoundError as error:
            self.logger.critical(f'{file} not found.\n{error}')
        except json.decoder.JSONDecodeError as error:
            self.logger.critical(f'{file} is invalid.\n{error}')
        else:
            orion_object['id'] = self.ids[object_.lower()]
            orion_object['RefWorkstation']['value'] = self.ids['ws']
            orion_object['RefJob']['value'] = self.ids['job']
            Orion.update((orion_object))

    def get_ids(self, ws):
        self.ids['ws'] = ws['id']
        self.ids['job'] = ws['RefJob']['value']
        if not Orion.exists(self.ids['job']):
            raise ValueError('Critical: object does not exist in Orion: {self.ids["job"]}')
        self.ids['oee'] = ws['RefOEE']['value']
        self.ids['throughput'] = ws['RefThroughput']['value']

    def calculate_KPIs(self):
        oeeCalculator = OEECalculator(self.ids['ws'])
        oeeCalculator.prepare(self.con)
        oee = oeeCalculator.calculate_OEE()
        throughput = oeeCalculator.calculate_throughput()
        return oee, throughput

    def handle_ws(self, ws):
        self.ids = self.blank_ids.copy()
        self.get_ids(ws)
        self.logger.info(f'Calculating KPIs for {ws["id"]}')
        oee, throughput = self.calculate_KPIs()
        Orion.update((oee, throughput))

    def handle(self):
        try:
            self.engine = create_engine(f'postgresql://{conf["postgresUser"]}:{conf["postgresPassword"]}@{conf["postgresHost"]}:{conf["postgresPort"]}')
            self.con = self.engine.connect()
            self.workstations = Orion.getWorkstations()
            if len(self.workstations) == 0:
                self.logger.critical(f'No Workstation is found in the Orion broker, no OEE data')
            for ws in self.workstations:
                self.handle_ws(ws)

        except (AttributeError,
                KeyError,
                RuntimeError,
                TypeError,
                ValueError,
                ZeroDivisionError,
                psycopg2.OperationalError,
                sqlalchemy.exc.OperationalError) as error:
            # could not calculate OEE or Throughput
            # try to delete the OEE and Throughput values, if we have enough data
            self.logger.error(error)
            if None in self.ids.values():
                self.logger.critical(f'A critical error occured, not even the ids of the objects could be determined. No OEE data. An OEE and a Throughput object should be cleared, but it cannot be determined, which ones.')
            else:
                self.logger.error(f'An error happened, trying to clear all attributes of the OEE and Throughput objects.')
                for object_ in ('OEE', 'Throughput'):
                    self.delete_attributes(object_)
        finally:
            try:
                self.con.close()
                self.engine.dispose()
            except NameError:
                pass

