"""A file for functions called in many objects derived from unittest.TestCase
"""
# Standard Library imports
import json
import glob
import os
import sys
import unittest

# PyPI imports
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.types import Text

# Custom imports
sys.path.insert(0, os.path.join("..", "app"))
import OEE
from modules import reupload_jsons_to_Orion

# Constants
WORKSTATION_ID = "urn:ngsiv2:i40Asset:Workstation:001"
WORKSTATION_TABLE = WORKSTATION_ID.lower().replace(":", "_") + "_i40asset"
OEE_ID = "urn:ngsiv2:i40Asset:OEE1"
OEE_TABLE = OEE_ID.lower().replace(":", "_") + "_i40asset"
WORKSTATION_FILE = f"{WORKSTATION_TABLE}.csv"
JOB_ID = "urn:ngsiv2:i40Process:Job:000001"
JOB_TABLE = JOB_ID.lower().replace(":", "_") + "_i40process"
JOB_FILE = f"{JOB_TABLE}.csv"
PLACES = 5

# Load environment variables
POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_SCHEMA = os.environ.get("POSTGRES_SCHEMA")


def setupClass_common(cls: unittest.TestCase):
    """The common parts of the setupClasses of the test_OEE, test_LoopHandler and test_main classes

    Args:
        cls (unittest.testCase): testCase object, 
        the setupClass of which is to be extended with this function
    """
    # cls.maxDiff = None
    cls.logger.debug("setupClass_common runs")
    reupload_jsons_to_Orion.main()
    cls.engine = create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}"
    )
    cls.con = cls.engine.connect()
    if not cls.engine.dialect.has_schema(cls.engine, POSTGRES_SCHEMA):
        cls.engine.execute(sqlalchemy.schema.CreateSchema(POSTGRES_SCHEMA))

    cls.oee_template = OEE.OEECalculator(WORKSTATION_ID)


    """
    Read and upload both the Workstation and the Job test logs to PostgreSQL
    Then download them to ensure that the data types of the downloaded table
    Match those the data types under real time conditions
    """
    # read test logs
    cls.workstation_df = pd.read_csv(os.path.join("csv", WORKSTATION_FILE))

    # map to int, because the test data has ".0"s appended
    cls.workstation_df["recvtimets"] = cls.workstation_df["recvtimets"].map(int)
    # upload workstation logs
    cls.workstation_df.to_sql(
        name=WORKSTATION_TABLE,
        con=cls.con,
        schema=POSTGRES_SCHEMA,
        index=False,
        # Fiware Cygnus uses Text by default for all columns
        dtype=Text,
        if_exists="replace",
    )
    # download test logs
    cls.workstation_df = pd.read_sql_query(
        sqlalchemy.text(f"select * from {POSTGRES_SCHEMA}.{WORKSTATION_TABLE}"), con=cls.con
    )

    # the same with the Job logs
    cls.job_df = pd.read_csv(os.path.join("csv", JOB_FILE))
    cls.job_df["recvtimets"] = cls.job_df["recvtimets"].map(int)
    cls.job_df.to_sql(
        name=JOB_TABLE,
        con=cls.con,
        schema=POSTGRES_SCHEMA,
        index=False,
        dtype=Text,
        if_exists="replace",
    )
    cls.job_df = pd.read_sql_query(
        sqlalchemy.text(f"select * from {POSTGRES_SCHEMA}.{JOB_TABLE}"), con=cls.con
    )

    # load all jsons into a dict for easy access
    cls.jsons = {}
    jsons = glob.glob(os.path.join("..", "json", "*.json"))
    for file in jsons:
        json_name = os.path.splitext(os.path.basename(file))[0]
        with open(file, "r") as f:
            cls.jsons[json_name] = json.load(f)
