#!/bin/bash
echo "Testing can only be performed on GNU/Linux machines.
The testing process needs MOMAMS up and running on localhost. Please start it if you have not already.
Also, the tests delete and create objects in the Orion broker.
It also changes the PostgreSQL data.
The tests set environment variables according to the env file.
These environment variables must match those in the MOMAMS docker-compose.yml file.
Never use the tests in a production environment.
Do you still want to proceed? [yN]"
read ans
if [ $ans = 'y' ]; then
    source env
    python test_object_to_template.py
    python test_Orion.py
    python test_OEE.py
    python test_LoopHandler.py
    python test_main.py
else
    echo "Exiting..."
fi

