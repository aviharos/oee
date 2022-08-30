# -*- coding: utf-8 -*-
# Standard Library imports
from datetime import datetime
import os

# PyPI packages
import numpy as np
import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy.types import DateTime, Float, BigInteger, Text

# custom imports
from Logger import getLogger
from object_to_template import object_to_template
import Orion


class OEECalculator:
    """
    An OEE calculator class that builds on Fiware Cygnus logs.
    It uses milliseconds for the time unit, just like Cygnus.

    Purpose:
        Calculating OEE and throughput data

    Disclaimer:
        The OEECalculator class does not consider multiple jobs per shift.
        If a shift contains multiple jobs, the calculations will
        be done as if the shift started when the last job started.

    Usage:
        Configure your Orion JSONS objects as in the json directory.
        The Workstation refers to the Job,
            the OEE object
            the Throughput object
        The Job refers to the Part
            and the Operation inside the Part.
        There is also an OperatorSchedule object.

    Inputs:
        workstation_id: the Orion id of the Workstation object,
        operatorSchedule_id: the Orion id of the OperatorSchedule object.

    Methods:
        __init__(workstation_id):
            Inputs:
                workstation_id:
                    The Orion id of the workstation

        set_time():
            Inputs:
                None
            sets the OEECalculator's time
            this method is split from the others for testing purposes

        prepare(con):
            Inputs:
                con:
                    the sqlalchemy module's engine's connection object
            gets data from the Orion broker
                Cygnus logs
            configures itself for today's shift
            if the object cannot prepare, it clears
            all attributes of the OEE object
                Possible reasons:
                    invalid Orion objects (see JSONS objects)
                    conectivity issues
                    the current time does not fall within the shift

        calculate_OEE():
            output:
                the Orion OEE object (see sample in JSON objects)

        calculate_throughput():
            output:
                the Orion Throughput object (see sample in JSONS objects)

    The preferred way to 
    """

    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
    col_dtypes = {
        "recvtimets": BigInteger(),
        "recvtime": DateTime(),
        "availability": Float(),
        "performance": Float(),
        "quality": Float(),
        "oee": Float(),
        "throughput_shift": Float(),
        "job": Text(),
    }
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

    def __init__(self, workstation_id):
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
        """ "
        Cygnus uses UTC regardless of timezone by default
        the OEE Calculator uses local timezone
        but since the timestamps are not affected,
        and the OEECalculator uses only the timestamps for
        calculations, there is no need to use timezone information
        """
        self.now = datetime.now()

    def now_unix(self):
        return self.now.timestamp() * 1000

    def msToDateTimeString(self, ms):
        return str(datetime.fromtimestamp(ms / 1000.0).strftime(self.DATETIME_FORMAT))[
            :-3
        ]

    def msToDateTime(self, ms):
        return self.stringToDateTime(self.msToDateTimeString(ms))

    def stringToDateTime(self, string):
        return datetime.strptime(string, self.DATETIME_FORMAT)

    def timeToDatetime(self, string):
        return datetime.strptime(
            str(self.now.date()) + " " + string, "%Y-%m-%d %H:%M:%S"
        )

    def datetimeToMilliseconds(self, datetime_):
        return datetime_.timestamp() * 1000

    def convertRecvtimetsToInt(self, df):
        df["recvtimets"] = df["recvtimets"].astype("float64").astype("int64")

    def get_cygnus_postgres_table(self, orion_obj):
        return (
            orion_obj["id"].replace(":", "_").lower() + "_" + orion_obj["type"].lower()
        )

    def get_ws(self):
        self.ws["orion"] = Orion.get(self.ws["id"])
        self.ws["postgres_table"] = self.get_cygnus_postgres_table(self.ws["orion"])
        self.logger.debug(f"Workstation: {self.ws}")

    def get_operatorSchedule(self):
        try:
            self.operatorSchedule["id"] = self.ws["orion"]["RefOperatorSchedule"][
                "value"
            ]
        except (KeyError, TypeError) as error:
            raise KeyError(
                f'Critical: RefOperatorSchedule not foundin Workstation object :\n{self.ws["orion"]}.'
            ) from error
        self.operatorSchedule["orion"] = Orion.get(self.operatorSchedule["id"])
        self.logger.debug(f"OperatorSchedule: {self.operatorSchedule}")

    def is_datetime_in_todays_shift(self, datetime_):
        if datetime_ < self.today["OperatorWorkingScheduleStartsAt"]:
            return False
        if datetime_ > self.today["OperatorWorkingScheduleStopsAt"]:
            return False
        return True

    def get_todays_shift_limits(self):
        try:
            for time_ in (
                "OperatorWorkingScheduleStartsAt",
                "OperatorWorkingScheduleStopsAt",
            ):
                self.today[time_] = self.timeToDatetime(
                    self.operatorSchedule["orion"][time_]["value"]
                )
        except (ValueError, KeyError, TypeError) as error:
            raise ValueError(
                f"Critical: could not convert time in {self.operatorSchedule}."
            ) from error
        self.logger.debug(f"Today: {self.today}")

    def get_job_id(self):
        try:
            return self.ws["orion"]["RefJob"]["value"]
        except (KeyError, TypeError) as error:
            raise KeyError(
                f'The workstation object {self.ws["id"]} has no valid RefJob attribute:\nObject:\n{self.ws["orion"]}'
            )

    def get_job(self):
        self.job["id"] = self.get_job_id()
        self.job["orion"] = Orion.get(self.job["id"])
        self.job["postgres_table"] = self.get_cygnus_postgres_table(self.job["orion"])
        self.logger.debug(f"Job: {self.job}")

    def get_part_id(self):
        try:
            part_id = self.job["orion"]["RefPart"]["value"]
        except (KeyError, TypeError) as error:
            raise KeyError(
                f'Critical: RefPart not found in the Job {self.job["id"]}.\nObject:\n{self.job["orion"]}'
            ) from error
        self.part["id"] = part_id

    def get_part(self):
        self.get_part_id()
        self.logger.debug(f'Part id: {self.part["id"]}')
        self.part["orion"] = Orion.get(self.part["id"])
        # self.logger.debug(f'Part: {self.part}')

    def get_operation(self):
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
            raise KeyError(
                f'Invalid part or job specification. The current operation could not be resolved. See the JSON objects for reference.\nJob:\n{self.job["orion"]}\nPart:\n{self.part["orion"]}'
            ) from error
        # self.operation['id'] = self.operation['orion']['id']
        self.logger.debug(f"Operation: {self.operation}")

    def get_objects_shift_limits(self):
        self.get_ws()
        self.get_operatorSchedule()
        self.get_todays_shift_limits()
        self.get_job()
        self.get_part()
        self.get_operation()

    def get_query_start_timestamp(self, how):

        if how == "from_midnight":
            # construct midnight's datetime
            return self.datetimeToMilliseconds(datetime.combine(self.now.date(), datetime.min.time()))
        elif how == "from_schedule_start":
            return self.datetimeToMilliseconds(
                self.today["OperatorWorkingScheduleStartsAt"]
            )
        else:
            raise ValueError(
                f"Cannot set query start time. Invalid argument: how={how}"
            )

    def query_todays_data(self, con, table_name, how):
        start_timestamp = self.get_query_start_timestamp(how)
        try:
            df = pd.read_sql_query(
                f"""select * from {self.POSTGRES_SCHEMA}.{table_name}
                                       where {start_timestamp} <= cast (recvtimets as bigint)
                                       and cast (recvtimets as bigint) <= {self.now_unix()};""",
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

    def convert_dataframe_to_str(self, df):
        """
        Cygnus 2.16.0 uploads all data as Text to Postgres
        So with this version of Cygnus, this function is useless
        We do this to ensure that we can always work with strings to increase stability
        """
        return df.applymap(str)

    def sort_df_by_time(self, df_):
        # default: ascending order
        if df_["recvtimets"].dtype != np.int64:
            raise ValueError(
                f'The recvtimets column should contain np.int64 dtype values, current dtype: {df_["recvtimets"]}'
            )
        return df_.sort_values(by=["recvtimets"])

    def get_current_job_start_time_today(self):
        """
        If the the current job started in today's shift,
        return its start time,
        else return the shift's start time
        """
        df = self.ws["df"]
        job_changes = df[df["attrname"] == "RefJob"]

        if len(job_changes) == 0:
            # today's queried ws df does not contain a job change
            return self.today["OperatorWorkingScheduleStartsAt"]
        last_job = job_changes.iloc[-1]["attrvalue"]
        if last_job != self.job["id"]:
            raise ValueError(
                f"The last job in the Workstation object and the Workstation's PostgreSQL historic logs differ.\nWorkstation:\n{self.ws}\Last job in Workstation_logs:\n{last_job}"
            )
        last_job_change = job_changes.iloc[-1]["recvtimets"]
        return self.msToDateTime(last_job_change)

    def set_RefStartTime(self):
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
        self.set_now()
        try:
            # also includes getting the shift's limits
            self.get_objects_shift_limits()
        except (RuntimeError, KeyError, AttributeError, TypeError) as error:
            message = (
                f"Could not get and extract objects from Orion. Traceback:\n{error}"
            )
            self.logger.error(message)
            raise RuntimeError(message) from error

        if not self.is_datetime_in_todays_shift(self.now):
            raise ValueError(
                f"The current time: {self.now} is outside today's shift, no OEE data"
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

    def filter_in_relation_to_RefStartTime(self, df, how):
        if how == "after":
            filtered = df[
                df["recvtimets"]
                >= self.datetimeToMilliseconds(
                    self.today["RefStartTime"]
                )
            ]
        elif how == "before":
            filtered = df[
                df["recvtimets"]
                < self.datetimeToMilliseconds(
                    self.today["RefStartTime"]
                )
            ]
        else:
            raise ValueError("filter_RefStartTime: Invalid option how={how}")
        return filtered.reset_index(drop=True)

    def calc_availability_if_no_availability_record_after_RefStartTime(self, df_before):
        """
        the workstation's available attribute has not changed since the RefStartTime
        so the availability is 0 or 1 depending on if the Workstation is on or off
        since the df_av has at least one row, check the last row before RefStartTime
        """
        self.logger.debug(f"df_before:\n{df_before}")
        self.total_time_so_far_in_shift = self.now_unix() - self.datetimeToMilliseconds(self.today["RefStartTime"])
        df_before.sort_values(by=["recvtimets"], inplace=True)
        self.logger.debug(f"df_before.iloc[-1]['attrvalue']: {df_before.iloc[-1]['attrvalue']}")
        last_availability = df_before.iloc[-1]["attrvalue"]
        if last_availability == "true":
            # the Workstation is on since before RefStartTime
            self.total_available_time = self.total_time_so_far_in_shift
            return 1
        elif last_availability == "false":  # df_before.iloc[-1]["attrvalue"] == "false":
            # the Workstation is off since before RefStartTime
            self.total_available_time = 0
            return 0
        else:
            raise ValueError(f"Invalid Availability value: {last_availability} in postgres_table: {self.ws['postgres_table']} at recvtimets: {df_before.iloc[-1]['recvtimets']}")

    def calc_availability_if_exists_record_after_RefStartTime(self, df_after):
        # TODO check all instances of OperatorWorkingScheduleStartsAt if it should be RefStartTime instead
        df_after.sort_values(by=["recvtimets"], inplace=True)
        time_on = 0
        time_off = 0
        previous_timestamp = self.datetimeToMilliseconds(self.today["RefStartTime"])
        for _, row in df_after.iterrows():
            # self.logger.debug(f"row:\n{row}")
            """
            interate all rows
            check which interval is on and off, and increase times accordingly
            """
            current_timestamp = row["recvtimets"]
            interval_duration = current_timestamp - previous_timestamp
            # self.logger.debug(f"current_timestamp: {current_timestamp}")
            # self.logger.debug(f"previous_timestamp: {previous_timestamp}")
            # self.logger.debug(f"interval_duration: {interval_duration}")
            if row["attrvalue"] == "true":
                # the Workstation was turned on, so it was off in the previous interval
                time_off += interval_duration
            else:  # row["attrvalue"] == "false"
                # the Workstation was turned off, so it was on in the previous interval
                time_on += interval_duration
            # self.logger.debug(f"time_off: {time_off}")
            # self.logger.debug(f"time_on: {time_on}")
            previous_timestamp = current_timestamp

        # the interval from the last entry to now
        interval_duration = self.now_unix() - previous_timestamp
        if row["attrvalue"] == "true":
            # the Workstation is currently on
            time_on += interval_duration
        else:  # row["attrvalue"] == "false"
            # the Workstation is currently off
            time_off += interval_duration

        self.total_available_time = time_on
        self.total_time_so_far_in_shift = time_on + time_off
        if self.total_time_so_far_in_shift == 0:
            raise ZeroDivisionError("Total time so far in the shift is 0, no OEE data")
        return self.total_available_time / self.total_time_so_far_in_shift

    def calc_availability(self, df_av):
        """
        Available is true and false in this periodical order,
        starting with true
        we can sum the timestamps of the true values
        and the false values disctinctly, getting 2 sums
        the total available time is their difference
        """
        # filter for values starting from RefStartTime
        # self.logger.debug(f"df_av:\n{df_av}")
        df_after = self.filter_in_relation_to_RefStartTime(df_av, how="after")
        # self.logger.debug(f"df_after:\n{df_after}")
        if df_after.size == 0:
            self.logger.info("No Availability record found after RefStartTime: {self.today['RefStartTime']}, using today's previous availability records")
            df_before = self.filter_in_relation_to_RefStartTime(df_av, how="before")
            return self.calc_availability_if_no_availability_record_after_RefStartTime(df_before)
        else:
            # now it is sure that the df_after is not emtpy, at least one row
            return self.calc_availability_if_exists_record_after_RefStartTime(df_after)

    def handle_availability(self):
        """
        Important: the OEECalculator queries data from_midnight
        So if a Workstation becomes available before the schedule starts,
        The OEECalculator will recognise that it is available
        But the availability calculations will not consider any time before
        the schedule starts. The OEECalculator treats the Workstation
        as if it became available just when the schedule started.
        """
        df = self.ws["df"]
        df_av = df[df["attrname"] == "Available"]
        available_true = df_av[df_av["attrvalue"] == "true"]
        if available_true.size == 0:
            raise ValueError(
                f'The Workstation {self.ws["id"]} was not turned Available by {self.now} since midnight, no OEE data'
            )
        self.oee["Availability"]["value"] = self.calc_availability(df_av)

    def count_nonzero_unique(self, unique_values):
        if "0" in unique_values:
            # need to substract 1, because '0' does not represent a successful moulding
            # for example: ['0', '8', '16', '24'] contains 4 unique values
            # but these mean only 3 successful injection mouldings
            return unique_values.shape[0] - 1
        else:
            return unique_values.shape[0]

    def count_injection_mouldings(self):
        df = self.job["df"]
        attr_name_val = df[["attrname", "attrvalue"]]
        good_unique_values = attr_name_val[
            attr_name_val["attrname"] == "GoodPartCounter"
        ]["attrvalue"].unique()
        reject_unique_values = attr_name_val[
            attr_name_val["attrname"] == "RejectPartCounter"
        ]["attrvalue"].unique()
        self.n_successful_mouldings = self.count_nonzero_unique(good_unique_values)
        self.n_failed_mouldings = self.count_nonzero_unique(reject_unique_values)
        self.n_total_mouldings = self.n_successful_mouldings + self.n_failed_mouldings

    def handle_quality(self):
        if self.job["df"].size == 0:
            raise ValueError(
                f'No job data found for {self.job["id"]} up to time {self.now} on day {self.today}, no OEE data'
            )
        self.count_injection_mouldings()
        if self.n_total_mouldings == 0:
            raise ValueError("No operation was completed yet, no OEE data")
        self.oee["Quality"]["value"] = (
            self.n_successful_mouldings / self.n_total_mouldings
        )

    def handle_performance(self):
        self.oee["Performance"]["value"] = (
            self.n_total_mouldings
            * self.operation["orion"]["OperationTime"]["value"]
            / self.total_available_time
        )

    def calculate_OEE(self):
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
        self.shiftLengthInMilliseconds = self.datetimeToMilliseconds(
            self.today["OperatorWorkingScheduleStopsAt"]
        ) - self.datetimeToMilliseconds(self.today["RefStartTime"])
        self.throughput["Throughput"]["value"] = (
            (self.shiftLengthInMilliseconds / self.operation["OperationTime"]["value"])
            * self.operation["PartsPerOperation"]["value"]
            * self.oee["OEE"]["value"]
        )
        self.logger.info(f"Throughput: {self.throughput}")
        return self.throughput
