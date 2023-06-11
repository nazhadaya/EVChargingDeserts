import pandas as pd
import sqlite3
from pathlib import Path
import numpy as np
import Geography as geo
import VEHViews as vw

evpath = Path('DB/evadoption.sqlite')
evcon = sqlite3.connect(evpath)

def list_tables_in_viewdb(con):
    tables = list(pd.read_sql_query("SELECT name FROM sqlite_master WHERE type = 'table'", con)['name'].unique())
    return tables

def tablename_from_id(id):
    df = vw.table_from_viewdb('Filters')
    filters = df[df['id'] == id].iloc[0].tolist()

    tablename = '_'.join(filters[1:])

    return tablename

def get105_id_name(id):
    tablename142 = tablename_from_id(id)
    filters = tablename142.split('_')

    table105name = '_'.join(['VEH0105',filters[1],'Total', filters[3], filters[4]])
    id_105 = list(vw.table_from_viewdb(table105name)['filter_id'].unique())[0]
    return id_105, table105name

def get_tables(id_142):
    tablename142 = tablename_from_id(id_142)
    (id_105, tablename105) = get105_id_name(id_142)

    df142 = vw.table_from_viewdb(tablename142)
    df105 = vw.table_from_viewdb(tablename105)

    return df142, df105, id_105

def adoption_join(df142, df105):
    #ONSSort NOT CONSISTENT FROM VIEW TO VIEW!!!!
    dfJoin = pd.merge(df142, df105, on = ['ONSCode','ONSGeo','DATE'], suffixes = ('_142','_105'))
    return dfJoin

def drop_ONSSort(dfJoin):
    dfJoin =  dfJoin.drop(['ONSSort_142','ONSSort_105'], axis = 1)
    return dfJoin

def get_EV_share(dfJoin):
    dfJoin['EVshare'] = 100*(dfJoin['EVCOUNT']/dfJoin['VEHCOUNT'])
    return dfJoin

def get_adoption_view(id_142):
    tablename = 'VEH0142_' + str(id_142) + '_EVShare'
    df_ev = pd.read_sql('SELECT * FROM \"'+ tablename + '\"', evcon)
    return df_ev