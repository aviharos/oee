# -*- coding: utf-8 -*-
"""A wrapper around the OEECalculator object

It uses environment variables for configuration

Environment variables:
    POSTGRES_USER
    POSTGRES_PASSWORD
    POSTGRES_HOST
    POSTGRES_PORT
If any of the previous environment variables (except the port)
is missing, a RuntimeError is raised
"""
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
    """A class for calculating all KPIs of each Workstation

    It connects to the Postgres database,
    Gets all workstations from the Orion broker
    Calculates the OEE and Throughput objects using the OEECalculator
    and updates the OEE and Throughput objects in Orion

    It also catches the OEECalculator object's exceptions and logs them
    """
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

    def get_ids(self, ws: dict):
        """Get the Id of the necessary Orion objects

        Ids:
            Workstation
            Job
            OEE
            Throughput

        Args:
            ws (dict): Orion workstation object

        Raises:
            ValueError: if the Workstation's Job does not exist in Orion
        """
        self.ids["ws"] = ws["id"]
        self.ids["job"] = ws["RefJob"]["value"]
        if not Orion.exists(self.ids["job"]):
            raise ValueError(
                f'Critical: object does not exist in Orion: {self.ids["job"]}'
            )
        self.ids["oee"] = ws["RefOEE"]["value"]
        self.ids["throughput"] = ws["RefThroughput"]["value"]

    def calculate_KPIs(self):
        """Calculate the OEE and the Throughput of the current Workstation

        Wraps the OEECalculator class

        Returns:
            Tuple: (oee, throughput):
                oee:
                    the OEE object to be uploaded to Orion
                throughput:
                    the Throughput object to be uploaded to Orion
        """
        oeeCalculator = OEECalculator(self.ids["ws"])
        oeeCalculator.prepare(self.con)
        oee = oeeCalculator.calculate_OEE()
        throughput = oeeCalculator.calculate_throughput()
        return oee, throughput

    def handle_ws(self, ws: dict):
        """Handle everything related to calculating and updating the OEE and Throughput of a Workstation

        After calculating the OEE and the Throughput, these are alo updated in the Orion broker

        Args:
            ws (dict): Orion Workstation
        """ 
        self.ids = copy.deepcopy(self.blank_ids)
        self.get_ids(ws)
        self.logger.info(f'Calculating KPIs for {ws["id"]}')
        oee, throughput = self.calculate_KPIs()
        Orion.update([oee, throughput])

    def delete_attributes(self, object_: str):
        """Delete all attributes of the OEE or Throughput object in Orion

        This is needed only if calculating the OEE and Throughput
        fails for some reason or they are not cannot be calculated. 

        If the OEECalculator fails, or
        if there is no shift for a specific Workstation, the OEE and Throughput values
        are also cleared.

        The method is called for the OEE and the Throughput both.

        Args:
            object_ (str): either "OEE" or "Throughput"

        Raises:
            NotImplementedError: if the arg object_ is not supported
            FileNotFoundError: if the OEE.json or the Throughput.json is not found
            json.decoder.JSONDecodeError: if the OEE.json of the Throughput.json file cannot be decoded
        """
        if object_ not in ("OEE", "Throughput"):
            raise NotImplementedError(f"Delete attributes: unsupported object: {object_}")
        file = f"{object_}.json"
        try:
            orion_object = object_to_template(os.path.join("..", "json", file))
        except FileNotFoundError as error:
            self.logger.critical(f"Critical: {file} not found.\n{error}")
            raise
        except json.decoder.JSONDecodeError as error:
            self.logger.critical(f"Critical: {file} is invalid.\n{error}")
            raise
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
            Orion.update([orion_object])
            self.logger.info(f"Object cleared successfully: {orion_object}")

    def handle(self):
        """A function for handling all the Workstations

        This function creates the Postgres engine and the connection,
        and also disposes and closes them respectively.
        It wraps the handle_ws function

        If the OEECalculator throws any exception,
        this function tries to delete all KPI values using the function delete_attributes
        """
        self.engine = create_engine(
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}"
        )
        try:
            with self.engine.connect() as self.con:
                self.workstations = Orion.getWorkstations()
                if len(self.workstations) == 0:
                    self.logger.critical(
                        "Critical: no Workstation is found in the Orion broker, no OEE data"
                    )
                self.logger.info(f"Workstation objects found in Orion: {self.workstations}")
                for ws in self.workstations:
                    self.handle_ws(ws)

        except (
            AttributeError,
            KeyError,
            NotImplementedError,
            RuntimeError,
            TypeError,
            ValueError,
            ZeroDivisionError,
            psycopg2.OperationalError,
            sqlalchemy.exc.OperationalError
        ) as error:
            # could not calculate OEE or Throughput
            # try to delete the OEE and Throughput values, if we have enough data
            self.logger.error(error)
            if None in self.ids.values():
                self.logger.critical(
                    "Critical: an error occured, not even the ids of the objects could be determined. No OEE data. An OEE and a Throughput object should be cleared, but it cannot be determined, which ones."
                )
            else:
                self.logger.error(
                    "Error: an error happened, trying to clear all attributes of the OEE and Throughput objects."
                )
                for object_ in ("OEE", "Throughput"):
                    self.delete_attributes(object_)
                self.logger.info(
                    "Cleared OEE and Throughput."
                )
        finally:
            self.engine.dispose()
