# -*- coding: utf-8 -*-
"""
Install dependencies in Miniconda environment (just once)
conda install python=3.8 spyder sqlalchemy pandas psycopg2
"""

# Standard Library imports
from datetime import datetime
import os

# PyPI packages
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.schema import CreateSchema
from sqlalchemy.types import Text

# Custom imports, config
from conf import conf
from OEE import *
from Orion import *

global conf, postgresSchema, day

postgresSchema = conf['postgresSchema']
day = datetime(2022, 4, 4)

def loop():
    global engine, con
    engine = create_engine('postgresql://{}:{}@localhost:5432'.format(conf['postgresUser'], conf['postgresPassword']))
    con = engine.connect()

    workstationIds = getWorkstationIdsFromOrion()
    for workstationId in workstationIds:
        jobId = getActiveJobId(workstationId)
        # availability, performance, quality, oee
        oeeData = calculateOEE(day, JobId)
        #oee = pd.DataFrame.from_dict({'day': mai nap, ' availability':availability, 'performance':performance, 'quality':quality, 'oee':oee})
        if oeeData is not None:
            updateOEE(workstation, *oeeData)

    eldöntendő kérdés: egy nap csak egyszer szerepeljen az adatbázisban, vagy loggoljuk, hogy a nap folyamán hogyan változott az OEE, és annak komponensei?

    con.close()
    engine.dispose()

def main:
    # the app will be a microservice, and will call loop periodically
    loop

if __name__ == '__main__':
    main()

