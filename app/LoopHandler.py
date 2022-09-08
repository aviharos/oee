# -*- coding: utf-8 -*-
# Standard Library imports
import copy
import json
import os

# PyPI packages
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2

# Custom imports
from Logger import getLogger
from object_to_template import object_to_template
from OEE import OEECalculator
import Orion


class LoopHandler:
    logger = getLogger(__name__)
    blank_ids = {"ws": None, "job": None, "oee": None, "throughput": None}
    # Load environment variables
    POSTGRES_USER = os.environ.get("POSTGRES_USER")
    if POSTGRES_USER is None:
        raise RuntimeError("Critical: POSTGRES_USER environment variable is not set.")

    POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
    if POSTGRES_PASSWORD is None:
        raise RuntimeError(
            "Critical: POSTGRES_PASSWORD environment variable is not set."
        )

    POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
    if POSTGRES_HOST is None:
        raise RuntimeError("Critical: POSTGRES_HOST environment variable is not set.")

    POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
    if POSTGRES_PORT is None:
        POSTGRES_PORT = 5432
        logger.warning(
            f"POSTGRES_PORT environment variable is not set, using default: {POSTGRES_PORT}"
        )

    def __init__(self):
        self.ids = copy.deepcopy(self.blank_ids)

    def get_ids(self, ws):
        self.ids["ws"] = ws["id"]
        self.ids["job"] = ws["RefJob"]["value"]
        if not Orion.exists(self.ids["job"]):
            raise ValueError(
                f'Critical: object does not exist in Orion: {self.ids["job"]}'
            )
        self.ids["oee"] = ws["RefOEE"]["value"]
        self.ids["throughput"] = ws["RefThroughput"]["value"]

    def calculate_KPIs(self):
        oeeCalculator = OEECalculator(self.ids["ws"])
        oeeCalculator.prepare(self.con)
        oee = oeeCalculator.calculate_OEE()
        throughput = oeeCalculator.calculate_throughput()
        return oee, throughput

    def handle_ws(self, ws):
        self.ids = copy.deepcopy(self.blank_ids)
        self.get_ids(ws)
        self.logger.info(f'Calculating KPIs for {ws["id"]}')
        oee, throughput = self.calculate_KPIs()
        Orion.update([oee, throughput])

    def delete_attributes(self, object_):
        file = f"{object_}.json"
        try:
            orion_object = object_to_template(os.path.join("..", "json", file))
        except FileNotFoundError as error:
            self.logger.critical(f"{file} not found.\n{error}")
        except json.decoder.JSONDecodeError as error:
            self.logger.critical(f"{file} is invalid.\n{error}")
        else:
            orion_object["id"] = self.ids[object_.lower()]
            orion_object["RefWorkstation"]["value"] = self.ids["ws"]
            orion_object["RefJob"]["value"] = self.ids["job"]
            if object_ == "OEE":
                orion_object["Availability"]["value"] = None
                orion_object["Performance"]["value"] = None
                orion_object["Quality"]["value"] = None
                orion_object["OEE"]["value"] = None
            if object_ == "Throughput":
                orion_object["ThroughputPerShift"]["value"] = None
            self.logger.debug(f"Delete attributes, object: {orion_object}")
            Orion.update([orion_object])

    def handle(self):
        self.engine = create_engine(
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}"
        )
        try:
            with self.engine.connect() as self.con:
                self.workstations = Orion.getWorkstations()
                if len(self.workstations) == 0:
                    self.logger.critical(
                        "No Workstation is found in the Orion broker, no OEE data"
                    )
                for ws in self.workstations:
                    self.handle_ws(ws)

        except (
            AttributeError,
            KeyError,
            RuntimeError,
            TypeError,
            ValueError,
            ZeroDivisionError,
            psycopg2.OperationalError,
            sqlalchemy.exc.OperationalError,
        ) as error:
            # could not calculate OEE or Throughput
            # try to delete the OEE and Throughput values, if we have enough data
            self.logger.error(error)
            if None in self.ids.values():
                self.logger.critical(
                    "A critical error occured, not even the ids of the objects could be determined. No OEE data. An OEE and a Throughput object should be cleared, but it cannot be determined, which ones."
                )
            else:
                self.logger.warning(
                    "An error happened, trying to clear all attributes of the OEE and Throughput objects."
                )
                for object_ in ("OEE", "Throughput"):
                    self.delete_attributes(object_)
                self.logger.warning(
                    "Cleared OEE and Throughput."
                )
        finally:
            self.engine.dispose()
