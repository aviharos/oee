# -*- coding: utf-8 -*-
"""
Install dependencies in Miniconda environment (just once)
conda install python=3.8 spyder sqlalchemy pandas psycopg2
"""

# Standard Library imports

# PyPI packages
from sqlalchemy import create_engine

# Custom imports, config
from conf import conf
import OEE
import Orion

global conf, postgresSchema

postgresSchema = conf['postgresSchema']

def loop():
    global engine, con
    engine = create_engine('postgresql://{}:{}@localhost:5432'.format(conf['postgresUser'], conf['postgresPassword']))
    con = engine.connect()

    workstationIds = Orion.getWorkstationIdsFromOrion()
    for workstationId in workstationIds:
        jobId = Orion.getActiveJobId(workstationId)
        # availability, performance, quality, oee, throughput
        oeeData = OEE.calculateOEE(workstationId, jobId)
        if oeeData is not None:
            OEE.updateOEE(workstationId, *oeeData)

    con.close()
    engine.dispose()

def main():
    # the app will be a microservice, and will call loop periodically
    loop

if __name__ == '__main__':
    main()

