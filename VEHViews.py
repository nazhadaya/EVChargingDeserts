import pandas as pd
import sqlite3
from pathlib import Path
import numpy as np
import Geography as geo

vehdb_path = Path('DB/vehdb.sqlite')
viewdb_path = Path('DB/views.sqlite')
geodb_path = Path('DB/geography.sqlite')

def connect_to_database():
    conn_vehdb = sqlite3.connect(vehdb_path)
    cur_vehdb = conn_vehdb.cursor()
    return conn_vehdb, cur_vehdb

def connect_to_viewdb():
    conn_vehdb = sqlite3.connect(viewdb_path)
    cur_vehdb = conn_vehdb.cursor()
    return conn_vehdb, cur_vehdb

def table_from_vehdb(tablename):
    con = connect_to_database()[0]
    cur = con.cursor()
    df = pd.read_sql('SELECT * FROM \"'+ tablename + '\"', con)
    return df

def table_from_viewdb(tablename):
    con = connect_to_viewdb()[0]
    cur = con.cursor()
    df = pd.read_sql('SELECT * FROM \"'+ tablename + '\"', con)
    return df

def input_filters():
    
    filters = list()

    filters.append(input('BodyType Filter: '))
    filters.append(input('Fuel Filter: '))
    filters.append(input('Keepership Filter: '))
    filters.append(input('QEND Filter: '))
    
    return filters

def check_filters(filters):
    #needs updating for shortened fuels and new "Hybrid category"....
    uniqueBodyType = list(table_from_vehdb('BodyType')['bodytype'].unique())
    uniqueFuel = list(table_from_vehdb('Fuel')['fuel'].unique())
    uniqueKeepership = list(table_from_vehdb('Keepership')['keepership'].unique())
    uniqueQend = list(table_from_vehdb('Qend')['qend'].unique())

    ListofLists = list([uniqueBodyType, uniqueFuel, uniqueKeepership, uniqueQend])

    for i in range(len(filters)):
        if filters[i] in ListofLists[i]:
            print(f'{filters[i]} is a match!')
        else:
            return 'ERROR! Filter {filters[i]} not a match, please use a filter in {ListofLists[i]}'
    return 'All filters match!'

def make_view_filtered(df, filters):
    dfcopy = df[(df['BodyType'] == filters[0]) & (df['Fuel']==filters[1]) & (df['Keepership'] == filters[2])& (df['QEND'] == filters[3])].copy()
    dfcopy = dfcopy.reset_index(drop=True)
    return dfcopy

def add_new_filterID(newVIEWname, con, cur):

    filters = newVIEWname.split('_')
    print(newVIEWname)
    print(filters)

    cur.execute(''' INSERT OR IGNORE INTO Filters (VEH_id, BodyType_id, Fuel_id, Keepership_id, Quarter_id)
        VALUES ( ?, ?, ?, ?, ? ) ''', (filters[0], filters[1], filters[2], filters[3], filters[4]))
    cur.execute('SELECT id FROM Filters WHERE (VEH_id = ? AND BodyType_id = ? AND Fuel_id = ? AND Keepership_id = ? AND Quarter_id = ?)', (filters[0], filters[1], filters[2], filters[3], filters[4]))
    filter_id = cur.fetchone()[0]

    print(filter_id)
    try:
        cur.execute('ALTER TABLE \"' + newVIEWname + '\" ADD filter_id INTEGER NOT NULL default 0')

        cur.execute('UPDATE \"' + newVIEWname + '\" SET filter_id = filter_id + ' + str(filter_id))

        print('This table alread has a filter_id defined! \n\n')

    except:
        print(f'Resetting filter_id to new id of {filter_id}\n\n')
        cur.execute('UPDATE \"' + newVIEWname + '\" SET filter_id = ' + str(filter_id))
    con.commit()

def get_LA_codes():
    dfLAs = geo.table_from_geodb('LAs')
    LAs = list(dfLAs['Census_Code'].unique())
    return LAs

def filter_for_LAs(df, LAcodes):
    dfcopy = dfcopy[dfcopy['Census_Code'].isin(LAcodes)]
    return dfcopy

def drop_filtered_columns(dfcopy):
    dfcopy = dfcopy.drop(columns = ['BodyType','Fuel','Keepership','QEND'])
    return dfcopy

def get_filename(oldtablename, filters):
    newtablename = '_'.join(filters).replace(' ','_')
    newtablename = oldtablename + newtablename
    return newtablename

#Run Above Twice if a join is what you seek and use "adoption_join". Otherwise, skip to saving new view to database.

def shorten_fuels(df):
    #Largely for VEH142 to get shorter names
    df['Fuel'] = df['Fuel'].replace('Battery electric','BEV',regex=True)
    df['Fuel'] = df['Fuel'].replace('Plug-in hybrid electric \(diesel\)','PHEV-D',regex=True)
    df['Fuel'] = df['Fuel'].replace('Plug-in hybrid electric \(petrol\)','PHEV-P',regex=True)
    df['Fuel'] = df['Fuel'].replace('Other fuels','Other',regex=True)
    return df

#Method that merges the diesl plug-in with petrol plug-in and also the 'other' fuels category for the VEH0142 database (since a review of VEH0141 concluded 99% of other fuels are Range Extender EVs)
def merge_Hybrid(df):
    df = shorten_fuels(df)

    df['Fuel'] = df['Fuel'].replace('PHEV.\w','Hybrid',regex = True)
    df['Fuel'] = df['Fuel'].replace('Other','Hybrid',regex = True)
    # see bug entry in Notion.... removed 'ONSSort' from groupby columns
    df = df.groupby(['BodyType','Fuel','Keepership','ONSCode','ONSGeo','QEND','DATE'])['EVCOUNT'].sum().reset_index()
    
    return df
    
def merge_PHEV(df):
    df = shorten_fuels(df)

    df['Fuel'] = df['Fuel'].replace('PHEV.\w','PHEV',regex = True)
    # see bug entry in Notion.... removed 'ONSSort' from groupby columns
    df = df.groupby(['BodyType','Fuel','Keepership','ONSCode','ONSGeo','QEND','DATE'])['EVCOUNT'].sum().reset_index()
    
    return df

def adoption_join(df1, df2):
    dfJoin = pd.merge(df1, df2, on = ['ONSCode','ONSGeo'])
    return dfJoin

def commit_to_db(df, oldtablename, filters):
    con = connect_to_viewdb()[0]
    df.to_sql(get_filename(oldtablename, filters), con, if_exists = 'replace', index = False)

def show_tables_in_viewdb():
    table = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type = 'table'", connect_to_viewdb()[0])
    return table