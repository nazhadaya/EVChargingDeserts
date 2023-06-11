import pandas as pd
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
import re
import sqlite3

path = Path('WIP_Data/NCR_20220727_reduced.csv')
geopath = Path('DB/geography.sqlite')

df_ncr = pd.read_csv(path)
cols = list(df_ncr.columns)

def load_NCR():
    return df_ncr

def reduced_view(df):
    if 'Ownership' not in list(df.columns):
        reducedcols = ['fid', 'Census_Code', 'deviceOwnerName', 'deviceControllerName', 'deviceNetworks', 'chargeDeviceStatus', '3.0', '3.7', '7.0', '11.0', '22.0', '36.0', '43.0', '50.0', '75.0', '100.0', '120.0', '150.0', '175.0', '350.0', 'CONNECTORCOUNT', 'TOTALOUTPUTKW', 'INSTALLDATEPROXY','latitude', 'longitude','chargeDeviceID', 'deviceManufacturer', 'deviceModel','Name_2']
    else:
        reducedcols = ['fid', 'Census_Code', 'deviceOwnerName', 'deviceControllerName', 'deviceNetworks', 'chargeDeviceStatus', '3.0', '3.7', '7.0', '11.0', '22.0', '36.0', '43.0', '50.0', '75.0', '100.0', '120.0', '150.0', '175.0', '350.0', 'CONNECTORCOUNT', 'TOTALOUTPUTKW', 'INSTALLDATEPROXY','latitude', 'longitude','chargeDeviceID', 'deviceManufacturer', 'deviceModel','Name_2','Ownership']

    df_reduced = df[reducedcols]
    return df_reduced

def unique_values(df, key):
    unique_values = list(df[key].unique())
    return unique_values

#pulls a list of deviceOwners which are UTLAs, councils, etc. just LAs in general
def CSOwnershipLists():
    owners = unique_values(df_ncr, 'deviceOwnerName')
    LAOwners = [item for item in owners if 'City' in item if 'Council' in item if 'Borough' in item if 'Combined Authority' in item if 'Milton Keynes' in item if '\*No Owner' not in item]
    nonLAOwners = [item for item in owners if 'City' not in item if 'Council' not in item if 'Borough' not in item if 'Combined Authority' not in item if 'Milton Keynes' not in item if '\*No Owner' not in item]
    return owners, LAOwners, nonLAOwners

# Adds a tag for LA-owned charging stations
def addPub_Pri_tag(df):
    owners = unique_values(df, 'deviceOwnerName')

    nonLAOwners = [item for item in owners if 'City' not in item if 'Council' not in item if 'Borough' not in item if 'Combined Authority' not in item if 'Milton Keynes' not in item if '\*No Owner' not in item]

    #Defining conditions to use in an np.select() statement
    conditions = [
            df['deviceOwnerName'].str.contains("Council|City|Borough|Combined Authority|Milton Keynes"),
            df['deviceOwnerName'].str.contains("\*No Owner"),
            df['deviceOwnerName'].isin(nonLAOwners)
            ]
    choices = ['LA Owned','No Data','Non LA Owned']

    df['Ownership'] = np.select(conditions, choices, default = 'forgot_me')

    print(f'Added an \'Ownership\' column. Summary of added values below: \n\n {df.Ownership.value_counts()}')

    return df

#ownership_pie still needs some work
def ownership_pie(df):

    counts = df.Ownership.value_counts()
    my_labels = list(counts.keys())

    fig, ax = plt.subplots()

    df.Ownership.value_counts().plot(kind='pie', figsize = (5,5), autopct = lambda p:f'{p:.2f}%', labels = None)
    ax.get_yaxis().set_visible(False)
    ax.set(title = 'Charging Station Ownership by count in the UK')
    ax.legend(loc = "lower right", labels = my_labels)
    plt.show(block=False)
    #needs work
    return fig, ax

def classify_station_output(df):
# method for classifying a charging station as Slow/Fast, Rapid/UltraRapid or mixed
# from Lit Review Table 6: slow/fast = 3-22kW, rapid/ultra-rapid = 50+kW
    conditions = [
            ((df['3.0'] != 0) | (df['3.7'] != 0) | (df['7.0'] != 0) | (df['11.0'] != 0) | (df['22.0'] != 0)) & ((df['36.0'] == 0) |(df['43.0'] == 0) |(df['50.0'] == 0) |(df['75.0'] == 0) | (df['100.0'] == 0) | (df['120.0'] == 0) | (df['150.0'] == 0) | (df['175.0'] == 0) | (df['350.0'] == 0)),
            ((df['3.0'] == 0) | (df['3.7'] == 0) | (df['7.0'] == 0) | (df['11.0'] == 0) | (df['22.0'] == 0)) & ((df['36.0'] != 0) |(df['43.0'] != 0) |(df['50.0'] != 0) |(df['75.0'] != 0) | (df['100.0'] != 0) | (df['120.0'] != 0) | (df['150.0'] != 0) | (df['175.0'] != 0) | (df['350.0'] != 0)),
            ((df['3.0'] != 0) | (df['3.7'] != 0) | (df['7.0'] != 0) | (df['11.0'] != 0) | (df['22.0'] != 0)) & ((df['36.0'] != 0) |(df['43.0'] != 0) |(df['50.0'] != 0) |(df['75.0'] != 0) | (df['100.0'] != 0) | (df['120.0'] != 0) | (df['150.0'] != 0) | (df['175.0'] != 0) | (df['350.0'] != 0))
            ]
    choices = ['SF','RU','MIX']

    df['Output_Class'] = np.select(conditions, choices, default = 'NA')
    print(f'Added an \'Output_Class\' column. Summary of added values below: \n\n {df.Output_Class.value_counts()}')

    return df

# below is for already post-processed NCR file - need second
# method for OG NCR files (use "connectorXOutput" cols and find their unique values)
def get_unique_P():
    cols = list(df_ncr.columns)
    powercols = [item for item in cols if re.search('^(0|[1-9]\d*)(\.\d+)?$', item)]
    df_ncr
    return powercols

#With power outputs as columns,the values for each charging station corespond to a count for that CS power output. This method looks through the power output columns and returns the column name for the highest non-zero column at a single charging station
def get_highest_output(NCR):
    powercols = get_unique_P()

    powercols = ['chargeDeviceID'] + powercols[0:]

    m = NCR[powercols].set_index(['chargeDeviceID'],append=True)
    NCR['Highest_kW'] = (m.ne(0).dot(m.columns+',').str.rstrip(',').str.split(',').str[-1].droplevel(['chargeDeviceID']))
    NCR['Highest_kW'] = pd.to_numeric(NCR['Highest_kW'])
    return NCR

#an attempt to extract useful pricing data from the NCR - however, no indicator of how often this field in particular was being updated within the DataBase
def pricing():
    NCRfull = df_ncr
    price_info = NCRfull[(NCRfull['paymentRequiredDetails'].str.contains('£')) | (NCRfull['paymentRequiredDetails'].str.contains('kWh')) | (NCRfull['paymentRequiredDetails'].str.contains('^(0|[1-9]\d*)(\.\d+)?$', regex = True))]
    num_LAs_with_price_info = len(list(price_info['Census_Code'].unique()))
    unique_price_info = list(price_info['paymentRequiredDetails'].unique())
    # carry on later... some potential but most likely, these were added when the chargers were uploaded and haven't been updated since....

def add_CTY(NCR):
    geocon = sqlite3.connect(geopath)
    LAs = pd.read_sql_query('SELECT * FROM LAs',geocon)

    NCR = pd.merge(NCR,LAs[['Census_Code','CTYCODE']],on = 'Census_Code',how='left')

    return NCR

#outputs a tidied up version of the NCR by carrying out the methods listed below this one
def NCR_tidiedup():
    ncr = load_NCR()
    ncr = reduced_view(ncr)
    ncr = addPub_Pri_tag(ncr)
    ncr = classify_station_output(ncr)
    ncr = get_highest_output(ncr)
    ncr = add_CTY(ncr)

    return ncr

#adding CS counts to the NCR and grouping by CTYCode, carrying over two sums of charging station outputs as well (one of the highest CS connector output, and one summing all connectors per CS)
def ncr_counts():
    ncrtidy = NCR_tidiedup()
    #below populates CTYCODE for LAs not in CTYs with their LA Code
    ncrtidy['CTYCODE'] = ncrtidy['CTYCODE'].fillna(ncrtidy['Census_Code'])
    
    # building a pivot table for counts and summed up outputs
    #see notes in Notion 31/07/2022 Vehicle Stats Analysis work
    #for idea on how to add indices and run more analysis of the dataset 
    #through more pivot tables
    pivot = pd.pivot_table(ncrtidy,
    index = ['CTYCODE'],
    aggfunc={'Highest_kW':np.sum, 'TOTALOUTPUTKW': np.sum,'CTYCODE':len}
    ).rename(columns={'CTYCODE':'CS_count'})
    
    return pivot

    #filtering:
    # for below get dfCars from emiles.py
    #pivot[pivot.index=='E09000003']
    # set subtraction used to check mismatch between dataframes
    #list(set(dfCars_max.ONSCode)-set(pivot.index))
    #list(set(pivot.index) - set(dfCars_max.ONSCode))
    

#Two methods that should output the NCR data by ONSCode and have HHI/LA-Owned CSs and Total CS count (HHI done with No Owner entries treated as multiple suppliers)

# Method that calculates the HHI for UTLAs using the NCR database (note the lambda function used and how it treats "No Owner" entries as multiple market players, and not 1 big player - for example, 
# if an LA has 100 Charging Stations with 50 having No Owner entries... this code treats them as 50 separate owners in the HHI calc, rather than 1 big player with 50% market share).
def NCR_HHI():
    ncrtidy = NCR_tidiedup()
    ncrtidy['CTYCODE'] = ncrtidy['CTYCODE'].fillna(ncrtidy['Census_Code'])
    Counts = ncrtidy.groupby(['CTYCODE']).size()

    #building a slightly more complex version of the NCR_Ownership method
    #essentially does the same where unique owners will end up as attribute columns

    dfHHI = pd.crosstab(index = ncrtidy['CTYCODE'], columns = ncrtidy['deviceOwnerName'])
    dfHHI['totalCount'] = Counts
    
    #reset index after adding the counts column!
    dfHHI = dfHHI.reset_index()

    cols = list(dfHHI.columns)
    players = cols[2:-1]

    def f(x):
        return sum((100*item)**2 for item in x)

    dfHHI['HHI'] = dfHHI.apply(lambda x: x[cols[1]]*100*100/(x['totalCount']**2) + f(x[players])/(x['totalCount']**2), axis = 1)

    return dfHHI

#Generating the LA Ownership percentages
def NCR_Ownership():
    ncrtidy = NCR_tidiedup()
    ncrtidy['CTYCODE'] = ncrtidy['CTYCODE'].fillna(ncrtidy['Census_Code'])
    # df = ncrtidy.groupby(['Census_Code'])['Ownership'].value_counts()
    # just find a way to pivot this new df (make the 3 LA stat values attributes and create a new DF grouped by LA)
    Counts = ncrtidy.groupby(['CTYCODE']).size()
    # below works to get result described in above comment
    # to filter to UTLA, switch 'Census_Code' with 'CTYCODE' below AND in COUNTS
    dfOwnership = pd.crosstab( index = ncrtidy['CTYCODE'], columns = ncrtidy['Ownership'])
    dfOwnership['totalCount'] = Counts
    
    #reset index after adding the counts column!
    dfOwnership = dfOwnership.reset_index()

    dfOwnership['LA_pct'] = 100*dfOwnership['LA Owned']/dfOwnership['totalCount']

    return dfOwnership

#Combining LA Ownership and HHI in one dataframe
def NCR_competition_summary():
    dfHHI = NCR_HHI()
    dfOwnership = NCR_Ownership()
    dfComp = pd.merge(dfOwnership, dfHHI, left_on = 'CTYCODE', right_on = 'CTYCODE')
    #build a method here that runs the 2 calcs above and gives out a df with ONSCode, Name, HHI and % LA-Owned
    
    dfComp = dfComp[['CTYCODE','LA_pct','HHI']]
    
    return dfComp