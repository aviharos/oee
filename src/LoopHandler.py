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
import os

# PyPI packages
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2

# Custom imports
from Logger import getLogger
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
        pass

    def calculate_KPIs(self, workstation_id: str):
        """Calculate the OEE and the Throughput of the current Workstation

        Wraps the OEECalculator class

        Args:
            workstation_id:
                The Orion Workstation object's id

        Returns:
            Tuple: (oee, throughput):
                oee:
                    the OEE object to be uploaded to Orion
                    format in self.OEE_template
                throughput:
                    the Throughput object to be uploaded to Orion
        """
        oeeCalculator = OEECalculator(workstation_id)
        oeeCalculator.prepare(self.con)
        oee = oeeCalculator.calculate_OEE()
        throughput = oeeCalculator.calculate_throughput()
        return oee, throughput

    def handle_workstation(self, workstation_id: str):
        """Handle everything related to calculating and updating the OEE and Throughput of a Workstation

        After calculating the OEE and the Throughput, these are alo updated in the Orion broker

        Args:
            workstation_id:
                The Orion Workstation object's id
        """ 
        try:
            self.logger.info(f'Calculating KPIs for {workstation_id}')
            oee, throughput = self.calculate_KPIs(workstation_id)
            Orion.update_attribute(workstation_id, "OEEObject", "OEE", oee)
            Orion.update_attribute(workstation_id, "ThroughputPerShift", "Number", throughput)
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
            self.logger.error(error)
            self.clear_KPIs(workstation_id)

    def clear_oee(self, workstation_id: str):
        """Clear OEE of a Workstation in case of an error 

        Args:
            workstation_id (str): the Workstation's Orion id 
        """
        Orion.update_attribute(workstation_id, "OEEObject", "OEE", OEECalculator.OEE_template.copy())
        self.logger.info(f"OEE cleared successfully for workstation: {workstation_id}")

    def clear_throughputPerShift(self, workstation_id: str):
        """Clear ThroughputPerShift of a Workstation in case of an error 

        Args:
            workstation_id (str): the Workstation's Orion id 
        """
        Orion.update_attribute(workstation_id, "ThroughputPerShift", "Number", None)
        self.logger.info(f"ThroughputPerShift cleared successfully for workstation: {workstation_id}")

    def clear_KPIs(self, workstation_id: str):
        """Clear OEE and ThroughputPerShift of a Workstation in case of an error 

        Args:
            workstation_id (str): the Workstation's Orion id 
        """
        self.logger.error(f"Trying to clear KPIs of workstation: {workstation_id}")
        self.clear_oee(workstation_id)
        self.clear_throughputPerShift(workstation_id)

    def clear_all_KPIs(self):
        """Clear OEE and ThroughputPerShift attributes of all Workstations in case of an error 
        """
        self.logger.error("Error: an error happened, trying to clear all KPIs.")
        for workstation in self.workstations:
            self.clear_KPIs(workstation["id"])

    def handle(self):
        """A function for handling the OEE and Throughput calculations of all Workstations

        This function creates the Postgres engine and the connection,
        and also disposes and closes them respectively.
        It wraps the handle_workstation function

        If the OEECalculator throws any exception,
        this function tries to delete all KPI values using the function delete_attributes
        """
        try:
            self.workstations = Orion.getWorkstations()
        except (RuntimeError, ValueError) as error:
            self.logger.error(f"Error: HTTP request to get all Workstation objects failed.\n{error}")
            return
        self.logger.info(f"Workstation objects found in Orion: {self.workstations}")
        if len(self.workstations) == 0:
            self.logger.critical(
                "Critical: no Workstation is found in the Orion broker, no OEE data"
            )
            return
        self.engine = create_engine(
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}"
        )
        try:
            with self.engine.connect() as self.con:
                for workstation in self.workstations:
                    self.handle_workstation(workstation["id"])

        except (
            psycopg2.OperationalError,
            sqlalchemy.exc.OperationalError
        ) as error:
            self.logger.error(error)
            self.clear_all_KPIs()
        finally:
            self.engine.dispose()

