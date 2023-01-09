import pandas as pd

df = pd.read_csv('urn_ngsi_ld_job_202200045_job.csv')
# today df, today: 2022 04 04
tdf = df[df['recvtimets'] < 1649109599000]

gp = tdf[tdf['attrname'] == 'goodPartCounter']
# ha van benne 0, akkor 1-gyel kevesebb
gp_unique = gp['attrvalue'].unique()
print(gp_unique)
if '0' in gp_unique:
    print('0 in the unique gp values')
    n_successful_moudings = gp_unique.shape[0] - 1
else:
    n_successful_moudings = gp_unique.shape[0]


rp = tdf[tdf['attrname'] == 'rejectPartCounter']
# ha van benne 0, akkor 1-gyel kevesebb
rp_unique = rp['attrvalue'].unique()
print(rp_unique)
if '0' in rp_unique:
    print('0 in the unique rp values')
    n_failed_moudings = rp_unique.shape[0] - 1
else:
    n_failed_moudings = rp_unique.shape[0]

print(f'Successful mouldings: {n_successful_moudings}')
print(f'Failed mouldings: {n_failed_moudings}')

