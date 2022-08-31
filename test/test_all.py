import sys
import unittest
from modules import reupload_jsons_to_Orion
from test_Orion import test_Orion
from test_OEE import test_OEECalculator


def main():
    ans = input("""Testing can only be performed on GNU/Linux machines.
The testing process needs MOMAMS(https://github.com/aviharos/momams) up and running on localhost.
Please start it if you have not already.
Also, the tests delete and create objects in the Orion broker and change the PostgreSQL data.
The tests set environment variables according to the env file.
These environment variables must match those in the MOMAMS docker-compose.yml file.
Never use the tests in a production environment.
If you have not sourced the env file, please exit the tests, and source it:
$ source env
Do you want to proceed? [yN]""")
    if ans not in ["y", "yes"]:
        print("Exiting...")
        sys.exit(0)
    else:
        reupload_jsons_to_Orion.main()
        unittest.main()


if __name__ == "__main__":
    main()
