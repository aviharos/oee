# -*- coding: utf-8 -*-
"""This file contains the OEE and Throughput calculator class
"""
# Standard Library imports
from datetime import datetime
import os

# PyPI packages
import numpy as np
import pandas as pd
import psycopg2
import sqlalchemy

# custom imports
from Logger import getLogger
from object_to_template import object_to_template
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
            the OperatorSchedule (RefOperatorSchedule).
        The Job refers to the Part (RefPart),
            and the Operation inside the Part (CurrentOperationType).
        The operatorSchedule contains the schedule of the Workstation.

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
        oee:
            the Workstation's OEE object (that will eventually be uploaded to Orion)
        throughput:
            the Workstation's throughput object (that will eventually be uploaded to Orion)

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
    OEE_template = object_to_template(os.path.join("..", "json", "OEE.json"))
    Throughput_template = object_to_template(
        os.path.join("..", "json", "Throughput.json")
    )
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
        self.oee = self.OEE_template
        self.throughput = self.Throughput_template
        self.today = {}

        self.operatorSchedule = self.object_.copy()

        self.ws = self.object_.copy()
        self.ws["id"] = workstation_id

        self.job = self.object_.copy()

        self.part = self.object_.copy()

        self.operation = self.object_.copy()

    def __repr__(self):
        return f'OEECalculator({self.ws["id"]})'

    def __str__(self):
        return f'OEECalculator object for workstation: {self.ws["id"]}'

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

    def get_ws(self):
        """Download the Workstation object from Orion, get the table name of PostgreSQL logs"""
        self.ws["orion"] = Orion.get(self.ws["id"])
        self.ws["postgres_table"] = self.get_cygnus_postgres_table(self.ws["orion"])
        self.logger.debug(f"Workstation: {self.ws}")

    def get_operatorSchedule(self):
        """Get the OperatorSchedule object of the Workstation from orion

        Raises:
            KeyError or TypeError if the id cannot be read from the Workstation Orion object
        """
        try:
            self.operatorSchedule["id"] = self.ws["orion"]["RefOperatorSchedule"][
                "value"
            ]
        except (KeyError, TypeError) as error:
            raise error.__class__(
                f'Critical: RefOperatorSchedule not found in Workstation object :\n{self.ws["orion"]}.'
            ) from error
        self.operatorSchedule["orion"] = Orion.get(self.operatorSchedule["id"])
        self.logger.debug(f"OperatorSchedule: {self.operatorSchedule}")

    def is_datetime_in_todays_shift(self, datetime_: datetime):
        """Check if datetime is in todays shift

        Args:
            datetime_ (datetime): datetime object to check

        Returns:
            True if the datetime is within the shift of the Workstation
            False otherwise
        """
        if datetime_ < self.today["OperatorWorkingScheduleStartsAt"]:
            return False
        if datetime_ > self.today["OperatorWorkingScheduleStopsAt"]:
            return False
        return True

    def get_todays_shift_limits(self):
        """Get the limits of today's schedule

        Extract the schedule's start and end OperatorSchedule object
        Store the date in self.today

        Raises:
            ValueError, KeyError or TypeError if getting the schedule
                start or end or converting the time to datetime fails
        """
        try:
            for time_ in (
                "OperatorWorkingScheduleStartsAt",
                "OperatorWorkingScheduleStopsAt",
            ):
                self.today[time_] = self.timeToDatetime(
                    self.operatorSchedule["orion"][time_]["value"]
                )
        except (ValueError, KeyError, TypeError) as error:
            raise error.__class__(
                f"Critical: could not convert time: {time_} in {self.operatorSchedule}."
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
            return self.ws["orion"]["RefJob"]["value"]
        except (KeyError, TypeError) as error:
            raise error.__class__(
                f'The workstation object {self.ws["id"]} has no valid RefJob attribute:\nObject:\n{self.ws["orion"]}'
            ) from error

    def get_job(self):
        """Get Job from Orion, fill the self.job dict"""
        self.job["id"] = self.get_job_id()
        self.job["orion"] = Orion.get(self.job["id"])
        self.job["postgres_table"] = self.get_cygnus_postgres_table(self.job["orion"])
        self.logger.debug(f"Job: {self.job}")

    def get_part_id(self):
        """Get the referenced Part's Orion id from the Job Orion object

        Returns:
            the Part's Orion id (str)

        Raises:
            KeyError or TypeError if getting the value from the dict fails
        """
        try:
            part_id = self.job["orion"]["RefPart"]["value"]
        except (KeyError, TypeError) as error:
            raise KeyError(
                f'Critical: RefPart not found in the Job {self.job["id"]}.\nObject:\n{self.job["orion"]}'
            ) from error
        self.part["id"] = part_id

    def get_part(self):
        """Get Part from Orion, store in self.part dict"""
        self.get_part_id()
        self.logger.debug(f'Part id: {self.part["id"]}')
        self.part["orion"] = Orion.get(self.part["id"])
        # self.logger.debug(f'Part: {self.part}')

    def get_operation(self):
        """Get Operation, store in self.operation["orion"]

        The operations are stored in the Part object's Operations list

        Raises:
            KeyError:
                if the Part does not contain the the operation with the same OperationType
                    as the Job's CurrentOperationType
            AttributeError, KeyError or TypeError:
                if the reading the Job's CurrentOperationType fails
        """
        found = False
        try:
            for operation in self.part["orion"]["Operations"]["value"]:
                if (
                    operation["OperationType"]["value"]
                    == self.job["orion"]["CurrentOperationType"]["value"]
                ):
                    found = True
                    self.operation["orion"] = operation
                    break
            if not found:
                raise KeyError(
                    f'The part {self.part["orion"]} has no operation with type {self.job["orion"]["CurrentOperationType"]}'
                )
        except (AttributeError, KeyError, TypeError) as error:
            raise error.__class__(
                f'Invalid part or job specification. The current operation could not be resolved. See the JSON objects for reference.\nJob:\n{self.job["orion"]}\nPart:\n{self.part["orion"]}'
            ) from error
        # self.operation['id'] = self.operation['orion']['id']
        self.logger.debug(f"Operation: {self.operation}")

    def get_objects_shift_limits(self):
        """Get objects from Orion

        Fills the following dicts:
            self.ws
            self.job
            self.part
            self.operation
        """
        self.get_ws()
        self.get_operatorSchedule()
        self.get_todays_shift_limits()
        self.get_job()
        self.get_part()
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
                self.today["OperatorWorkingScheduleStartsAt"]
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
        df = self.ws["df"]
        refJob_entries = df[df["attrname"] == "RefJob"]

        if len(refJob_entries) == 0:
            # today's queried ws df does not contain a job change
            # we assume that the Job was started in a previous shift
            # so the job's today part started at the schedule start time today
            return self.today["OperatorWorkingScheduleStartsAt"]

        last_job = refJob_entries.iloc[-1]["attrvalue"]
        if last_job != self.job["id"]:
            raise ValueError(
                f"The last job in the Workstation object and the Workstation's PostgreSQL historic logs differ.\nWorkstation:\n{self.ws}\Last job in Workstation_logs:\n{last_job}"
            )
        current_Jobs_RefJob_entries = refJob_entries[refJob_entries["attrvalue"] == self.job["id"]]
        last_job_change = current_Jobs_RefJob_entries["recvtimets"].min()
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
                f'The current job started in this shift, updated RefStartTime: {self.today["RefStartTime"]}'
            )
        else:
            self.today["RefStartTime"] = self.today["OperatorWorkingScheduleStartsAt"]
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
                    referenced OperatorSchedule

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

        self.ws["df"] = self.query_todays_data(
            con=con, table_name=self.ws["postgres_table"], how="from_midnight"
        )
        self.ws["df"] = self.convert_dataframe_to_str(self.ws["df"])
        self.convertRecvtimetsToInt(self.ws["df"])
        self.ws["df"] = self.sort_df_by_time(self.ws["df"])

        self.job["df"] = self.query_todays_data(
            con=con, table_name=self.job["postgres_table"], how="from_schedule_start"
        )
        self.job["df"] = self.convert_dataframe_to_str(self.job["df"])
        self.convertRecvtimetsToInt(self.job["df"])
        self.job["df"] = self.sort_df_by_time(self.job["df"])

        self.oee["id"] = self.ws["orion"]["RefOEE"]["value"]
        self.oee["RefWorkstation"]["value"] = self.ws["id"]
        self.oee["RefJob"]["value"] = self.job["id"]

        self.throughput["id"] = self.ws["orion"]["RefThroughput"]["value"]
        self.throughput["RefWorkstation"]["value"] = self.ws["id"]
        self.throughput["RefJob"]["value"] = self.job["id"]

        self.set_RefStartTime()

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
            return 1
        elif (
            last_availability == "false"
        ):  # df_before.iloc[-1]["attrvalue"] == "false":
            # the Workstation is off since before RefStartTime
            self.total_available_time = 0
            return 0
        else:
            raise ValueError(
                f"Invalid Availability value: {last_availability} in postgres_table: {self.ws['postgres_table']} at recvtimets: {df_before.iloc[-1]['recvtimets']}"
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
        # see if we can determine the Workstation's Available attribute 
        # from RefStartTime to the first entry
        if len(df_before) == 0:
            # assume not turned on 
            # the OEE microservice's specification declares that 
            # the Workstation cannot be turned on before the schedule start!
            available = False
        else:
            # there is at least one Available attribute entry today before RefStartTime
            # use the last of those to see if the Workstation was on at RefStartTime
            df_before.sort_values(by=["recvtimets"], inplace=True)
            last_availability = df_before.iloc[-1]["attrvalue"]
            available = True if last_availability == "true" else False

        for _, row in df_after.iterrows():
            """
            Interate all rows
            Every 2 subsequent rows defines an interval during which the Workstation was on
            Or off without interruption
            The first row shows the Workstation's Available attribute during that interval
            Check which interval is on and off, and increase times accordingly

            Caution: Cygnus logs in a way that more subsequent rows can have the same
            Available value, so we cannot assume that they alternate

            The first and last intervals are special.
            The first interval starts at RefStartTime, ends at the first entry
            The last interval starts at the last entry, ends at self.now_unix

            The first interval is handled here, because the previous_timestamp
            starts at RefStartTime
            """
            current_timestamp = row["recvtimets"]
            interval_duration = current_timestamp - previous_timestamp
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
        if available:
            # the previous entry stated that the Workstation was on
            time_on += interval_duration
        else:
            # the previous entry stated that the Workstation was off
            time_off += interval_duration

        self.total_available_time = time_on
        self.total_time_so_far_since_RefStartTime = time_on + time_off
        if self.total_time_so_far_since_RefStartTime == 0:
            raise ZeroDivisionError("Total time so far in the shift is 0, no OEE data")
        return self.total_available_time / self.total_time_so_far_since_RefStartTime

    def calc_availability(self, df_av: pd.DataFrame):
        """Calculate the availability of the Workstation

        The Workstation's Available attribute
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
                "No Availability record found after RefStartTime: {self.today['RefStartTime']}, using today's previous availability records"
            )
            return self.calc_availability_if_no_availability_record_after_RefStartTime(
                df_before
            )
        else:
            # now it is sure that the df_after is not emtpy, at least one row
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
        df = self.ws["df"]
        df_av = df[df["attrname"] == "Available"]
        available_true = df_av[df_av["attrvalue"] == "true"]
        if available_true.size == 0:
            raise ValueError(
                f'The Workstation {self.ws["id"]} was not turned Available by {self.now_datetime} since midnight, no OEE data'
            )
        self.oee["Availability"]["value"] = self.calc_availability(df_av)

    def count_nonzero_unique(self, unique_values: np.array):
        """Count nonzero unique values of an iterable

        Used for counting the number of successful and failed cycles
        "0" does not count for a successful or failed cycles, so it is discarded.
        for example: ['0', '8', '16', '24'] contains 4 unique values
        but these mean only 3 successful cycles

        Args:
            unique_values (iterable): iterable object, containing numbers in string format

        Returns:
            Integer:
                The number of unique values that are not 0
                This is the number of successful or failed cycles
        """
        if "0" in unique_values:
            # need to substract 1, because '0' does not represent a successful cycle
            return unique_values.shape[0] - 1
        else:
            return unique_values.shape[0]

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
        good_unique_values = attr_name_val[
            attr_name_val["attrname"] == "GoodPartCounter"
        ]["attrvalue"].unique()
        reject_unique_values = attr_name_val[
            attr_name_val["attrname"] == "RejectPartCounter"
        ]["attrvalue"].unique()
        self.logger.debug(f"good_unique values: {good_unique_values}")
        self.logger.debug(f"reject_unique values: {reject_unique_values}")
        self.n_successful_cycles = self.count_nonzero_unique(good_unique_values)
        self.n_failed_cycles = self.count_nonzero_unique(reject_unique_values)
        self.n_total_cycles = self.n_successful_cycles + self.n_failed_cycles

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
        self.oee["Quality"]["value"] = (
            self.n_successful_cycles / self.n_total_cycles
        )

    def handle_performance(self):
        """Handle everythin related to the Performance KPI

        Store the result in self.oee["Performance"]["value"]"""
        self.oee["Performance"]["value"] = (
            self.n_total_cycles
            * self.operation["orion"]["CycleTime"]["value"]
            * 1e3  # we count in milliseconds
            / self.total_available_time
        )

    def calculate_OEE(self):
        """Calculate the OEE

        Returns:
            self.oee: OEE Object that will eventually be uploaded to Orion
        """
        self.handle_availability()
        self.handle_quality()
        self.handle_performance()
        self.oee["OEE"]["value"] = (
            self.oee["Availability"]["value"]
            * self.oee["Performance"]["value"]
            * self.oee["Quality"]["value"]
        )
        self.logger.info(f"OEE data: {self.oee}")
        return self.oee

    def calculate_throughput(self):
        """Calculate the Throughput

        Returns:
            self.throughput: Throughput Object that will eventually be uploaded to Orion
        """
        self.shiftLengthInMilliseconds = self.datetimeToMilliseconds(
            self.today["OperatorWorkingScheduleStopsAt"]
        ) - self.datetimeToMilliseconds(self.today["RefStartTime"])
        self.throughput["ThroughputPerShift"]["value"] = (
            # use milliseconds
            (
                self.shiftLengthInMilliseconds
                / (self.operation["orion"]["CycleTime"]["value"] * 1e3)
            )
            * self.operation["orion"]["PartsPerCycle"]["value"]
            * self.oee["OEE"]["value"]
        )
        self.logger.info(f"Throughput: {self.throughput}")
        return self.throughput
