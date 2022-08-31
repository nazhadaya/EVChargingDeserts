import pandas as pd
from pathlib import Path
import sqlite3
import EVAdoption as ev
import VEHViews as vw

#EDS Dictionary based on Table 5 from ICCT paper
EDS = {'BEV_Total':{'min':1,'max':1},'Hybrid_Private':{'min':0.45,'max':0.49},'Hybrid_Company':{'min':0.11,'max':0.15}}

#EV average efficiency for UK vehicles... from EVdatabase website
kwh_mi = 0.316

#evadoptpath = Path('DB/evadoption.sqlite') 

#from downloaded LA traffic road stats
traffic_path = Path('WIP_Data/LA_traffic.csv')
#units in table for total distance travelled = miles

dftraffic = pd.read_csv(traffic_path)

#building dataframes using the above data and setting the year to selected year for study: 2019
def get_traffic_dfs():

    cols = list(dftraffic.columns)

    df_2019 = dftraffic[dftraffic['year'] ==  2019] #can make the year an input later
    newcols = [cols[2]] + [cols[1]] + [cols[5]] + cols[-2:]  #reordering and dropping columns
    
    df_2019 = df_2019[newcols].reset_index(drop = True)

    df_2019 = df_2019.rename(columns={'ons_code':'ONSCode'})

    df_2019cars = df_2019.drop(columns = ['all_motor_vehicles'])
    df_2019total = df_2019.drop(columns = ['cars_and_taxis'])

    return df_2019cars, df_2019total

#getting VEHTABLES view ids based on body type filter selected (Cars or Total only as these are the filters available for Road Traffic Stats)
def get_view_ids(body):
    BodyType = body
    view_ids = []
    df_t = []

    if BodyType == 'Cars':
        view_ids = [32, 31, 7]
        df_t = get_traffic_dfs()[0]
    elif BodyType == 'Total':
        view_ids = [35, 34, 19]
        df_t = get_traffic_dfs()[1]
    
    return view_ids, df_t

#Getting the corresponding filters based on a view ID input
def filters_from_id(id):
    df = vw.table_from_viewdb('Filters')
    filters = df[df['id'] == id].iloc[0].tolist()
    return filters

#Merging EV adoption calcs with traffic statistics
def traffic_adoption_merge(body):
    DF_views = []
    (view_ids, df_t) = get_view_ids(body)
    for item in view_ids:
        try:
            DF_views.append(ev.get_adoption_view(item).drop(['ONSSort'], axis = 1))
        except:
            DF_views.append(ev.get_adoption_view(item))

    newcolname = 'EVshare_' + str(view_ids[0]) #need to manually suffix first id onto the first df being joined to traffic df

    df_merged = pd.merge(df_t, DF_views[0], on = ['ONSCode'], suffixes = ('','_'+str(view_ids[0]))).merge(DF_views[1], on = ['ONSCode','ONSGeo','DATE'], suffixes = ('','_'+str(view_ids[1]))).merge(DF_views[2], on = ['ONSCode','ONSGeo','DATE'], suffixes = ('','_'+str(view_ids[2])))
    df_merged = df_merged.rename(columns={'EVshare': newcolname}) # rename moved post-merge to ensure the suffixes are there for other dfs (suffix only added if the column name is common in a merge)
    return df_merged

#Getting the EDS values from the ICCT table (manually built into this script as a dictionary)
def get_eds(id, minmax):
    filters = filters_from_id(id)
    eds = EDS[filters[3]+'_'+filters[4]][minmax]
    return eds

#Calculating emiles (DISCLAIMER: THIS METHOD WAS FOUND TO BE ERRONEUOUS AS IT USED CATEGORICAL EV SHARES AND NOT ABSOLUTE EV SHARES!!! SEE `fix_emiles' method below for a fix!!!!)
#some more detail on above: this method, for example, was using EV share for private hybrids based on a division of private hybrid EVs by total `private only` cars! To get private hybrid car contribution to total miles travelled, the division has to be done over ALL cars!! Therefore, `fix_emiles'
# was written and used moving forwards
def get_emiles(bodytype, minmax):
    df_merged = traffic_adoption_merge(bodytype)
    view_ids = get_view_ids(bodytype)[0]
# Below: EDS is already a fraction but EVShare values need to be /100!!!!
    try:
        df_merged['emiles'] = df_merged['all_motor_vehicles']*(df_merged['EVshare_'+str(view_ids[0])]*get_eds(view_ids[0],minmax)/100 + df_merged['EVshare_'+str(view_ids[1])]*get_eds(view_ids[1],minmax)/100 + df_merged['EVshare_'+str(view_ids[2])]*get_eds(view_ids[2],minmax)/100)
        # adding an "emilespct" column
        df_merged['emilespct'] = 100*(df_merged['emiles']/df_merged['all_motor_vehicles'])
    except: #trying for "all" then "cars and taxis" if a "key error" is triggered
        df_merged['emiles'] = df_merged['cars_and_taxis']*(df_merged['EVshare_'+str(view_ids[0])]*get_eds(view_ids[0],minmax)/100 + df_merged['EVshare_'+str(view_ids[1])]*get_eds(view_ids[1],minmax)/100 + df_merged['EVshare_'+str(view_ids[2])]*get_eds(view_ids[2],minmax)/100)
        df_merged['emilespct'] = 100*(df_merged['emiles']/df_merged['cars_and_taxis'])

    df_merged['Dem_MWh'] = df_merged['emiles']*kwh_mi/1000

    return df_merged

def fix_emiles(df):
    #Fixing the emiles and subsequent demand calculations
    #Currently only works for Cars Max (priority) - will need to be expanded to fix other combinations

    df['emiles'] = df['cars_and_taxis'] * (EDS['Hybrid_Private']['max']*df['EVCOUNT'] + EDS['Hybrid_Company']['max']*df['EVCOUNT_31'] + EDS['BEV_Total']['max']*df['EVCOUNT_7'])/df['VEHCOUNT_7']
    df['emilespct'] = 100*df['emiles']/df['cars_and_taxis']
    df['Dem_MWh'] = df['emiles']*kwh_mi/1000

    return df
    
#How to filter with "&":and and "|":or statements:
#df[(df['local_authority_name'] == 'Islington') & ((df['year'] == 2019) | (df['year'] == 2020))]