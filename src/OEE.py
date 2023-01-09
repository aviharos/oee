﻿# -*- coding: utf-8 -*-
"""This file contains the OEE and Throughput calculator class
"""
# Standard Library imports
import copy
from datetime import datetime
import os

# PyPI packages
import numpy as np
import pandas as pd
import psycopg2
import sqlalchemy

# custom imports
from Logger import getLogger
import Orion


class OEECalculator:
    """An OEE calculator class that builds on Fiware Cygnus logs.

    It uses milliseconds for the time unit, just like Cygnus.

    Purpose:
        Calculating OEE and throughput data

    Disclaimer:
        The OEECalculator class does not consider multiple jobs per shift.
        If a shift contains multiple jobs, the calculations will
        be done as if the shift started when the last job started.

    Basic data model:
        Configure your Orion JSONS objects as in the json directory.
        See the README for more info.
        The basic idea of the data model is as follows.
        The Workstation refers to the Job (RefJob),
            the OEE object (RefOEE),
            the Throughput object (RefThroughput),
            the Shift (RefShift).
        The Job refers to the Operation (RefOperation),

    Common usage:
        oeeCalculator = OEECalculator(workstation_id)
        oeeCalculator.prepare(con)
        oee, throughput = oeeCalculator.calculate_KPIs()

    Args:
        workstation_id:
            the Orion id of the Workstation
        con:
            The sqlalchemy module's engine's connection object to PostgreSQL

    Returns:
        oee (dict):
            the Workstation's OEE object (that will eventually be uploaded to Orion)
            format:
                {
                "OEE": None,
                "Availability": None,
                "Performance": None,
                "Quality": None
                }
        throughput (float):
            the Workstation's estimated throughput per shift (that will eventually be uploaded to Orion)

    Raises various errors if the calculation fails
    """

    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
    """
    The way the OEECalculator stores data is according to the object_ dict
    id (str):
        Orion id 
    orion (dict):
        Orion object
    postgres_table (str):
        postgres table name of the logs of the object
    df (pandas DataFrame):
        logs downloaded from PostgreSQL
    """
    object_ = {"id": None, "orion": None, "postgres_table": None, "df": None}
    logger = getLogger(__name__)
    OEE_template = {
        "OEE": None,
        "Availability": None,
        "Performance": None,
        "Quality": None
        }
    # get environment variables
    POSTGRES_SCHEMA = os.environ.get("POSTGRES_SCHEMA")
    if POSTGRES_SCHEMA is None:
        POSTGRES_SCHEMA = "default_service"
        logger.warning(
            f'POSTGRES_SCHEMA environment varialbe not found, using default: "{POSTGRES_SCHEMA}"'
        )

    def __init__(self, workstation_id: str):
        """The constructor of the OEECalculator class

        Args:
            workstation_id (str): the Workstation's id in Orion
        """
        self.oee = copy.deepcopy(self.OEE_template)
        self.throughput = None
        self.today = {}

        self.shift = self.object_.copy()

        self.workstation = self.object_.copy()
        self.workstation["id"] = workstation_id

        self.job = self.object_.copy()

        self.operation = self.object_.copy()

    def __repr__(self):
        return f'OEECalculator({self.workstation["id"]})'

    def __str__(self):
        return f'OEECalculator object for workstation: {self.workstation["id"]}'

    def set_now(self):
        """A module for setting and freezing the OEECalculator's timestamp

        Cygnus uses UTC timestamps in milliseconds regardless of timezone by default
        The OEE Calculator also uses does the same. 
        The human readable datetime objects are displayed locally.

        This method is called only once per calculation.
        """
        self.now_unix = datetime.now().timestamp() * 1e3

    @property
    def now_datetime(self):
        """Function for getting the frozen timestamp in milliseconds

        Returns:
            the oeeCalculator.now timestamp in milliseconds (numeric)
        """
        return self.msToDateTime(self.now_unix)

    def msToDateTimeString(self, ms: float):
        """Convert a timestamp in milliseconds to human readable format according to the DATETIME_FORMAT

        Args:
            ms (float): timestamp in milliseconds

        Returns:
            timestamp in human readable format (str)
        """
        return str(datetime.fromtimestamp(ms / 1000.0).strftime(self.DATETIME_FORMAT))[
            :-3
        ]

    def msToDateTime(self, ms: float):
        """Convert a timestamp in milliseconds to datetime object

        Args:
            ms (float): timestamp in milliseconds

        Returns:
            timestamp in datetime object
        """
        return self.stringToDateTime(self.msToDateTimeString(ms))

    def stringToDateTime(self, string: str):
        """Convert a datetime in DATETIME_FORMAT string format to a datetime.datetime object

        Args:
            string (str): datetime in string

        Returns:
            timestamp in datetime object
        """
        return datetime.strptime(string, self.DATETIME_FORMAT)

    def timeToDatetime(self, string: str):
        """Convert a time (no date component) in string format to datetime

        The date component is the date of the oeeCalculator.now

        Args:
            string (str): the time of the day in string

        Returns:
            datetime object
        """
        return datetime.strptime(
            str(self.now_datetime.date()) + " " + string, "%Y-%m-%d %H:%M:%S"
        )

    def datetimeToMilliseconds(self, datetime_):
        """Convert datetime to unix timestamp in milliseconds

        Args:
            datetime_ (datetime): datetime to convert

        Returns:
            unix timestamp in milliseconds
        """
        return datetime_.timestamp() * 1000

    def convertRecvtimetsToInt(self, df):
        df["recvtimets"] = df["recvtimets"].astype("float64").astype("int64")

    def get_cygnus_postgres_table(self, orion_obj: dict):
        """Get the table name of the PostgreSQL logs

        The table names are set by Fiware Cygnus, this method just recreates the table name

        Args:
            orion_obj (dict): Orion object

        Returns:
            postgres table name (str)
        """
        return (
            orion_obj["id"].replace(":", "_").lower() + "_" + orion_obj["type"].lower()
        )

    def get_workstation(self):
        """Download the Workstation object from Orion, get the table name of PostgreSQL logs"""
        self.workstation["orion"] = Orion.get(self.workstation["id"])
        self.workstation["postgres_table"] = self.get_cygnus_postgres_table(self.workstation["orion"])
        self.logger.debug(f"Workstation: {self.workstation}")

    def get_shift(self):
        """Get the Shift object of the Workstation from orion

        Raises:
            KeyError or TypeError if the id cannot be read from the Workstation Orion object
        """
        try:
            self.shift["id"] = self.workstation["orion"]["RefShift"][
                "value"
            ]
        except (KeyError, TypeError) as error:
            raise error.__class__(
                f'Critical: RefShift not found in Workstation object :\n{self.workstation["orion"]}.'
            ) from error
        self.shift["orion"] = Orion.get(self.shift["id"])
        self.logger.debug(f"Shift: {self.shift}")

    def is_datetime_in_todays_shift(self, datetime_: datetime):
        """Check if datetime is in todays shift

        Args:
            datetime_ (datetime): datetime object to check

        Returns:
            True if the datetime is within the shift of the Workstation
            False otherwise
        """
        if datetime_ < self.today["Start"]:
            return False
        if datetime_ > self.today["End"]:
            return False
        return True

    def get_todays_shift_limits(self):
        """Get the limits of today's schedule

        Extract the schedule's start and end Shift object
        Store the date in self.today

        Raises:
            ValueError, KeyError or TypeError if getting the schedule
                start or end or converting the time to datetime fails
        """
        try:
            for time_ in (
                "Start",
                "End",
            ):
                self.today[time_] = self.timeToDatetime(
                    self.shift["orion"][time_]["value"]
                )
        except (ValueError, KeyError, TypeError) as error:
            raise error.__class__(
                f"Critical: could not convert time: {time_} in {self.shift}."
            ) from error
        self.logger.debug(f"Today: {self.today}")

    def get_job_id(self):
        """Get the referenced Job's Orion id from the Workstation Orion object

        Returns:
            the Job's Orion id (str)

        Raises:
            KeyError or TypeError if getting the value from the dict fails
        """
        try:
            return self.workstation["orion"]["RefJob"]["value"]
        except (KeyError, TypeError) as error:
            raise error.__class__(
                f'The workstation object {self.workstation["id"]} has no valid RefJob attribute:\nObject:\n{self.workstation["orion"]}'
            ) from error

    def get_job(self):
        """Get Job from Orion, fill the self.job dict"""
        self.job["id"] = self.get_job_id()
        self.job["orion"] = Orion.get(self.job["id"])
        self.job["postgres_table"] = self.get_cygnus_postgres_table(self.job["orion"])
        self.logger.debug(f"Job: {self.job}")

    def get_operation_id(self):
        """Get the referenced Operation's Orion id from the Job Orion object

        Returns:
            the Operation's Orion id (str)

        Raises:
            KeyError or TypeError if getting the value from the dict fails
        """
        try:
            operation_id = self.job["orion"]["RefOperation"]["value"]
        except (KeyError, TypeError) as error:
            raise KeyError(
                f'Critical: RefOperation not found in the Job {self.job["id"]}.\nObject:\n{self.job["orion"]}'
            ) from error
        self.operation["id"] = operation_id

    def get_operation(self):
        """Get Operation from Orion, store in self.operation dict"""
        self.get_operation_id()
        self.logger.debug(f'operation id: {self.operation["id"]}')
        self.operation["orion"] = Orion.get(self.operation["id"])
        self.logger.debug(f'operation: {self.operation}')

    def get_objects_shift_limits(self):
        """Get objects from Orion

        Workstation
        Shift
        Today's shift limits
        Job
        Operation

        Fills the following dicts:
            self.workstation
            self.job
            self.operation
        """
        self.get_workstation()
        self.get_shift()
        self.get_todays_shift_limits()
        self.get_job()
        self.get_operation()

    def get_query_start_timestamp(self, how: str):
        """Get the PostgreSQL query's starting timestamp

        Args:
            how (str): "from_midnight" or "from_schedule_start"

        Returns:
            timestamp in milliseconds

        Raises:
            NotImplementedError:
                if the arg "how" is not of the two possible values
        """
        if how == "from_midnight":
            # construct midnight's datetime
            return self.datetimeToMilliseconds(
                datetime.combine(self.now_datetime.date(), datetime.min.time())
            )
        elif how == "from_schedule_start":
            return self.datetimeToMilliseconds(
                self.today["Start"]
            )
        else:
            raise NotImplementedError(
                f"Cannot set query start time. Unsupported argument: how={how}"
            )

    def query_todays_data(self, con, table_name: str, how: str):
        """Query today's data from PostgreSQL from a table

        Args:
            con (sqlalchemy connection object): self.con, the LoopHandler creates it
            table_name (str): PostgreSQL table name
            how (str): "from_midnight" or "from_schedule_start"

        Returns:
            pandas DataFrame containing the queried data

        Raises:
            RuntimeError:
                if the SQL query fails
        """
        start_timestamp = self.get_query_start_timestamp(how)
        self.logger.debug(f"query_todays_data: start_timestamp: {start_timestamp}")
        try:
            df = pd.read_sql_query(
                f"""select * from {self.POSTGRES_SCHEMA}.{table_name}
                                       where {start_timestamp} <= cast (recvtimets as bigint)
                                       and cast (recvtimets as bigint) <= {self.now_unix};""",
                con=con,
            )
        except (
            psycopg2.errors.UndefinedTable,
            sqlalchemy.exc.ProgrammingError,
        ) as error:
            raise RuntimeError(
                f"The SQL table: {table_name} cannot be queried from the table_schema: {self.POSTGRES_SCHEMA}."
            ) from error
        return df

    def convert_dataframe_to_str(self, df: pd.DataFrame):
        """Convert a pandas DataFrame's all columns to str

        Cygnus 2.16.0 uploads all data as Text to Postgres
        So with this version of Cygnus, this function is useless
        We do this to ensure that we can always work with strings to increase stability

        Returns:
            pandas DataFrame, all data converted to str
        """
        return df.applymap(str)

    def sort_df_by_time(self, df_: pd.DataFrame):
        """Sort a pandas DataFrame in ascending order by the timestamps

        Uses the column recvtimets

        Args:
            df_ (pd.DataFrame): pandas DataFrame to be sorted

        Returns:
            sorted pandas DataFrame

        Raises:
            ValueError:
                if the recvtimets column does not contain np.int64 dtype values
        """
        if df_["recvtimets"].dtype != np.int64:
            raise ValueError(
                f'The recvtimets column should contain np.int64 dtype values, current dtype: {df_["recvtimets"]}'
            )
        return df_.sort_values(by=["recvtimets"])

    def get_current_job_start_time_today(self):
        """Get the Job's start time. If it is before the schedule's start, return the schedule start time

        If the the current job started in today's shift,
        return its start time,
        else return the shift's start time

        Returns:
            datetime object

        Raises:
            ValueError:
                if the Workstation's RefJob attribute
                and the last job according to the Cygnus logs differ
        """
        df = self.workstation["df"]
        refJob_entries = df[df["attrname"] == "RefJob"]

        if len(refJob_entries) == 0:
            # today's queried workstation df does not contain a job change
            # we assume that the Job was started in a previous shift
            # so the job's today part started at the schedule start time today
            self.logger.debug(f"Today's job start time: {self.today['Start']}")
            return self.today["Start"]

        last_job = refJob_entries.iloc[-1]["attrvalue"]
        if last_job != self.job["id"]:
            raise ValueError(
                f"The last job in the Workstation object and the Workstation's PostgreSQL historic logs differ.\nWorkstation:\n{self.workstation}\Last job in Workstation_logs:\n{last_job}"
            )
        current_Jobs_RefJob_entries = refJob_entries[refJob_entries["attrvalue"] == self.job["id"]]
        last_job_change = current_Jobs_RefJob_entries["recvtimets"].min()
        self.logger.debug(f"Today's job start time: {last_job_change}")
        return self.msToDateTime(last_job_change)

    def set_RefStartTime(self):
        """Get RefStartTime

        RefStartTime is the one later in time of these two:
            1. Current Job's start time
            2. Current schedule's start time

        This way, only one Job will be considered (1.),
        and only one working interval in the schedule is considered (2.)

        As a consequence, the OEE and Throughput metrics always refer to the current Job
        and the current working interval of the schedule

        The result is stored in self.today["RefStartTime"]
        """
        current_job_start_time = self.get_current_job_start_time_today()
        if self.is_datetime_in_todays_shift(current_job_start_time):
            # the Job started in this shift, update RefStartTime
            self.today["RefStartTime"] = current_job_start_time
            self.logger.info(
                f'The current job started in this shift, RefStartTime: {self.today["RefStartTime"]}'
            )
        else:
            self.today["RefStartTime"] = self.today["Start"]
            self.logger.info(
                f'The current job started before this shift, RefStartTime: {self.today["RefStartTime"]}'
            )

    def prepare(self, con):
        """Prepare the OEECalculator object

        Download all Objects from Orion
        Set time data
        Query logs from Cygnus
        Sets RefStartTime

        Args:
            con (sqlalchemy connection object): LoopHandler creates it

        Raises:
            RuntimeError, KeyError, AttributeError or TypeError:
                if getting the Orion objects or reading their attributes fails
            ValueError:
                if the Workstation is currently not operational according to the
                    referenced Shift

        """
        self.set_now()
        try:
            # also includes getting the shift's limits
            self.get_objects_shift_limits()
        except (RuntimeError, KeyError, AttributeError, TypeError) as error:
            message = f"Could not get and extract objects from Orion."
            self.logger.error(message)
            raise error.__class__(message) from error

        if not self.is_datetime_in_todays_shift(self.now_datetime):
            raise ValueError(
                f"The current time: {self.now_datetime} is outside today's shift, no OEE data"
            )

        self.workstation["df"] = self.query_todays_data(
            con=con, table_name=self.workstation["postgres_table"], how="from_midnight"
        )
        self.workstation["df"] = self.convert_dataframe_to_str(self.workstation["df"])
        self.convertRecvtimetsToInt(self.workstation["df"])
        self.workstation["df"] = self.sort_df_by_time(self.workstation["df"])

        self.job["df"] = self.query_todays_data(
            con=con, table_name=self.job["postgres_table"], how="from_schedule_start"
        )
        self.job["df"] = self.convert_dataframe_to_str(self.job["df"])
        self.convertRecvtimetsToInt(self.job["df"])
        self.job["df"] = self.sort_df_by_time(self.job["df"])

        self.set_RefStartTime()

        # make sure that no job record is before RefStartTime
        # for example if someone turns on the Workstation before Start 
        # despite the documentation's clear statement about not to do that
        self.job["df"] = self.filter_in_relation_to_RefStartTime(self.job["df"], how="after")

    def filter_in_relation_to_RefStartTime(self, df: pd.DataFrame, how: str):
        """Filter Cygnus logs in relation to RefStartTime

        Args:
            df (pd.DataFrame): queried Cygnus logs
            how (str): "before" or "after"

        Returns:
            filtered Cygnus logs (pd.DataFrame)

        Raises:
            NotImplementedError:
                if the arg "how" differs from the 2 supported options
        """
        if how == "after":
            filtered = df[
                df["recvtimets"]
                >= self.datetimeToMilliseconds(self.today["RefStartTime"])
            ]
        elif how == "before":
            filtered = df[
                df["recvtimets"]
                < self.datetimeToMilliseconds(self.today["RefStartTime"])
            ]
        else:
            raise NotImplementedError(f"filter_RefStartTime: Invalid option how={how}")
        return filtered.reset_index(drop=True)

    def calc_availability_if_no_availability_record_after_RefStartTime(
        self, df_before: pd.DataFrame
    ):
        """Calculate availability if there is no availability record after RefStartTime in the Workstation logs

        The Workstation's available attribute has not changed since the RefStartTime.
        It was either on or off without a change ever since.
        So the availability is 0 or 1 depending on if the Workstation is on or off.
        Since today's workstation logs contains at least one row, check the last row before RefStartTime.
        The only possible explanation is that the Workstation was turned on or off
        before the RefStartTime and nothing changed since then.

        Also sets the total_available_time and total_time_so_far_since_RefStartTime attributes.

        Args:
            df_before (pd.DataFrame): today's queried Workstation Cygnus logs before RefStartTime

        Returns:
            1 if the Workstation was turned on before the RefStartTime,
            0 otherwise (the Workstation was turned off since RefStartTime)

        Raises:
            ValueError:
                if the Cygnus log contains an invalid Availability value
        """
        self.logger.debug(f"df_before:\n{df_before}")
        self.total_time_so_far_since_RefStartTime = self.now_unix - self.datetimeToMilliseconds(
            self.today["RefStartTime"]
        )
        df_before.sort_values(by=["recvtimets"], inplace=True)
        self.logger.debug(
            f"df_before.iloc[-1]['attrvalue']: {df_before.iloc[-1]['attrvalue']}"
        )
        last_availability = df_before.iloc[-1]["attrvalue"]
        if last_availability == "true":
            # the Workstation is on since before RefStartTime
            self.total_available_time = self.total_time_so_far_since_RefStartTime
            self.logger.info(f"The Workstation is on since RefStartTime. Total available time: {self.total_available_time}")
            return 1
        elif (
            last_availability == "false"
        ):  # df_before.iloc[-1]["attrvalue"] == "false":
            # the Workstation is off since before RefStartTime
            self.total_available_time = 0
            self.logger.info(f"The Workstation is off since RefStartTime. Total available time: {self.total_available_time}")
            return 0
        else:
            raise ValueError(
                f"Invalid Availability value: {last_availability} in postgres_table: {self.workstation['postgres_table']} at recvtimets: {df_before.iloc[-1]['recvtimets']}"
            )

    def calc_availability_if_exists_record_after_RefStartTime(
        self, df_before, df_after: pd.DataFrame
    ):
        """Calculate availability if there is at least one Availability record in the Cygnus logs since RefStartTime

        Args:
            df_after (pd.DataFrame): Workstation Cygnus logs after RefStartTime

        Returns:
            Availability: availability KPI (float)

        Raises:
            ZeroDivisionError:
                if the total_time_so_far_since_RefStartTime happens to be 0
        """
        df_after.sort_values(by=["recvtimets"], inplace=True)
        time_on = 0
        time_off = 0
        # the first interval starts at RefStartTime
        previous_timestamp = self.datetimeToMilliseconds(self.today["RefStartTime"])
        # see if we can determine the Workstation's available attribute 
        # from RefStartTime to the first entry
        if len(df_before) == 0:
            # assume not turned on 
            # the OEE microservice's specification declares that 
            # the Workstation cannot be turned on before the schedule start!
            available = False
        else:
            # there is at least one available attribute entry today before RefStartTime
            # use the last of those to see if the Workstation was on at RefStartTime
            df_before.sort_values(by=["recvtimets"], inplace=True)
            last_availability = df_before.iloc[-1]["attrvalue"]
            available = True if last_availability == "true" else False

        for _, row in df_after.iterrows():
            """
            Interate all rows
            Every 2 subsequent rows defines an interval during which the Workstation was on
            Or off without interruption
            The first row shows the Workstation's available attribute during that interval
            Check which interval is on and off, and increase times accordingly

            Caution: Cygnus logs in a way that more subsequent rows can have the same
            available value, so we cannot assume that they alternate

            The first and last intervals are special.
            The first interval starts at RefStartTime, ends at the first entry
            The last interval starts at the last entry, ends at self.now_unix

            The first interval is handled here, because the previous_timestamp
            starts at RefStartTime
            """
            current_timestamp = row["recvtimets"]
            interval_duration = current_timestamp - previous_timestamp
            self.logger.debug(f"Processing availability interval: previous timestamp: {previous_timestamp}, interval_duration: {interval_duration}, available: {available}")
            if available:
                # the previous entry stated that the Workstation was on
                time_on += interval_duration
            else:
                # the previous entry stated that the Workstation was off
                time_off += interval_duration
            # set available attribute for next interval
            available = row["attrvalue"] == "true"
            previous_timestamp = current_timestamp

        # last interval
        interval_duration = self.now_unix - previous_timestamp
        self.logger.debug(f"Processing availability interval: previous timestamp: {previous_timestamp}, interval_duration: {interval_duration}, available: {available}")
        if available:
            # the previous entry stated that the Workstation was on
            time_on += interval_duration
        else:
            # the previous entry stated that the Workstation was off
            time_off += interval_duration

        self.total_available_time = time_on
        self.logger.info(f"Total available time: {self.total_available_time}")
        self.total_time_so_far_since_RefStartTime = time_on + time_off
        self.logger.info(f"Total time so far since RefStartTime: {self.total_time_so_far_since_RefStartTime}")
        if self.total_time_so_far_since_RefStartTime == 0:
            raise ZeroDivisionError("Total time so far in the shift is 0, no OEE data")
        return self.total_available_time / self.total_time_so_far_since_RefStartTime

    def calc_availability(self, df_av: pd.DataFrame):
        """Calculate the availability of the Workstation

        The Workstation's available attribute
        is true and false in this periodical order.

        This function first checks if there is any availability log since RefStartTime
        Then uses either of the two functions:
            calc_availability_if_exists_record_after_RefStartTime
            calc_availability_if_no_availability_record_after_RefStartTime

        Args:
            df_av (pd.DataFrame): the Workstation's logs filtered for attrname == "Availability"

        Returns:
            Availability KPI (float)
        """
        df_before = self.filter_in_relation_to_RefStartTime(df_av, how="before")
        df_after = self.filter_in_relation_to_RefStartTime(df_av, how="after")
        if df_after.size == 0:
            self.logger.info(
                f"No Availability record found after RefStartTime: {self.today['RefStartTime']}, using today's previous availability records"
            )
            return self.calc_availability_if_no_availability_record_after_RefStartTime(
                df_before
            )
        else:
            # now it is sure that the df_after is not emtpy, at least one row
            self.logger.info(
                f"Found Availability record after RefStartTime: {self.today['RefStartTime']}"
            )
            return self.calc_availability_if_exists_record_after_RefStartTime(df_before, df_after)

    def handle_availability(self):
        """Handle everything related to the Availability KPI

        Important: the OEECalculator queries data from_midnight
        So if a Workstation becomes available before the schedule starts,
        The OEECalculator will recognise that it is available
        But the availability calculations will not consider any time before
        the schedule starts.
        The OEECalculator treats the Workstation
        as if it became available just when the schedule started.
        This way, the Availability KPI cannot exceed 1.

        The Availability is stored in self.oee["Availability"]["value"]

        Raises:
            ValueError:
                if the Workstation was not turned on since midnight
        """
        df = self.workstation["df"]
        df_av = df[df["attrname"] == "available"]
        available_true = df_av[df_av["attrvalue"] == "true"]
        if available_true.size == 0:
            raise ValueError(
                f'The Workstation {self.workstation["id"]} was not turned available by {self.now_datetime} since midnight, no OEE data'
            )
        self.oee["Availability"] = self.calc_availability(df_av)
        self.logger.info(f"Availability: {self.oee['Availability']}")

    def count_cycles_based_on_counter_values(self, values: np.array):
        """Count number of injection moulding cycles based on a np.array of goodPartCounter values

        Used for counting the number of successful or failed cycles
        Example:
            8 pcs of parts per cycle
            ['16', '24', '40', '56'] contains 4 values
            values '32' and '48' are missing,
            but since it is known that the 8 is added to the counter each cycle
            there must have been 6 cycles,
                including the one that made the counter 16 at first

        The module cannot handle if the min. or max. value is missing,
            for example in the previous case, '16' or '48' is missing from the logs

        The way this function counts is as follows:
            Converts the np.array to integer array
                raises ValueError if one item cannot be converted
            gets min and max values of the array
            gets PartsPerCycle value
            if 0 in np.array of counter values:
                result = (max-min)/PartsPerCycle
            if 0 not in np.array of counter values:
                result = (max-min)/PartsPerCycle + 1

        Args:
            values (iterable): iterable object, containing numbers in string format

        Returns:
            Integer:
                The number of successful or failed cycles
        """
        self.logger.debug(f"Count Workstation cycles based on counter values: {values}")
        values = np.unique(np.array(values))
        try:
            values = values.astype(int)
        except ValueError as error:
            raise ValueError("At least one goodPartCounter or rejectPartCounter value cannot be converted to int") from error
        min = values.min()
        max = values.max()
        if 0 in values:
            self.logger.debug("0 in values")
            return (max - min) / self.operation["orion"]["PartsPerCycle"]["value"]
        if 0 not in values:
            self.logger.debug("0 not in values")
            return (max - min) / self.operation["orion"]["PartsPerCycle"]["value"] + 1

    def count_cycles(self):
        """Count the number of successful and failed production cycles 

        In this terminology, a cycle consists of the Workstation creating
        self.operation["orion"]["PartsPerCycle"] pcs of parts,
        all of them good or reject parts.

        The values are stored in
            n_successful_cycles
            n_failed_cycles
            n_total_cycles"""
        df = self.job["df"]
        attr_name_val = df[["attrname", "attrvalue"]]
        goodPartCounter_values = attr_name_val[
            attr_name_val["attrname"] == "goodPartCounter"
        ]["attrvalue"].unique()
        rejectPartCounter_values = attr_name_val[
            attr_name_val["attrname"] == "rejectPartCounter"
        ]["attrvalue"].unique()
        self.logger.debug(f"goodPartCounter values: {goodPartCounter_values}")
        self.logger.debug(f"rejectPartCounter values: {rejectPartCounter_values}")
        self.n_successful_cycles = self.count_cycles_based_on_counter_values(goodPartCounter_values)
        self.logger.debug(f"Number of successful cycles: {self.n_successful_cycles}")
        self.n_failed_cycles = self.count_cycles_based_on_counter_values(rejectPartCounter_values)
        self.logger.debug(f"Number of failed cycles: {self.n_failed_cycles}")
        self.n_total_cycles = self.n_successful_cycles + self.n_failed_cycles
        self.logger.debug(f"Number of total cycles: {self.n_total_cycles}")

    def handle_quality(self):
        """Handle everything related to quality KPI

        Store the result in self.oee["Quality"]["value"]

        Raises:
            ValueError:
                No completed production cycle in the Job's logs"""
        if self.job["df"].size == 0:
            raise ValueError(
                f'No job data found for {self.job["id"]} up to time {self.now_datetime} on day {self.today}, no OEE data'
            )
        self.count_cycles()
        if self.n_total_cycles == 0:
            raise ValueError("No operation was completed yet, no OEE data")
        self.oee["Quality"] = (
            self.n_successful_cycles / self.n_total_cycles
        )
        self.logger.info(f"Quality: {self.oee['Quality']}")

    def handle_performance(self):
        """Handle everythin related to the Performance KPI

        Store the result in self.oee["Performance"]["value"]"""
        self.oee["Performance"] = (
            self.n_total_cycles
            * self.operation["orion"]["CycleTime"]["value"]
            * 1e3  # we count in milliseconds
            / self.total_available_time
        )
        self.logger.info(f"Performance: {self.oee['Performance']}")

    def calculate_OEE(self):
        """Calculate the OEE

        Returns:
            self.oee: OEE Object that will eventually be uploaded to Orion
        """
        self.handle_availability()
        self.handle_quality()
        self.handle_performance()
        self.oee["OEE"] = (
            self.oee["Availability"]
            * self.oee["Performance"]
            * self.oee["Quality"]
        )
        self.logger.info(f"OEE data: {self.oee}")
        return self.oee

    def calculate_throughput(self):
        """Calculate the Throughput

        Returns:
            self.throughput: Throughput Object that will eventually be uploaded to Orion
        """
        self.shiftLengthInMilliseconds = self.datetimeToMilliseconds(
            self.today["End"]
        ) - self.datetimeToMilliseconds(self.today["RefStartTime"])
        self.throughput = (
            # use milliseconds
            (
                self.shiftLengthInMilliseconds
                / (self.operation["orion"]["CycleTime"]["value"] * 1e3)
            )
            * self.operation["orion"]["PartsPerCycle"]["value"]
            * self.oee["OEE"]
        )
        self.logger.info(f"Throughput per shift (estimated): {self.throughput}")
        return self.throughput
