import emiles as em
from pathlib import Path
import Geography as geo
import pandas as pd
import NCRprocessing as ncr
import numpy as np
import ORCS as orcs
import IMD as imd

#Script for getting the "supply/demand CSU" value for upper tier Local Authorities

#getting the emiles tables
dfCars_min = em.get_emiles('Cars','min')
dfCars_max = em.get_emiles('Cars','max')

#CRUCIAL step below - NOTEE that fix_emiles currently only works for Cars Max (priority for MSc thesis to keep going. Other filter combos need to be included under the method!)
dfCars_max = em.fix_emiles(dfCars_max)

dfTotal_min = em.get_emiles('Total','min')
dfTotal_max = em.get_emiles('Total','max')

DF = [dfCars_min, dfCars_max, dfTotal_min, dfTotal_max]

LAs = geo.table_from_geodb('LAs')

LBOs = list(LAs[LAs['Area_Code'] == 'LBO']['CTYCODE'])

CAU_LAs = list(LAs[LAs['CTYCODE'] == 'E47000006']['Census_Code'])

#Details of CAU:
ONSCode = 'E47000006'
local_authority_name = 'Tees Valley Combined Authority'
year = 2019
ONSGeo = local_authority_name
DATE = DF[0]['DATE'][0]


#Find a better way using numpy select or something ..... Google is your friend

DFcon = []

#getting the NCR Counts pivot table and converting to a DataFrame
dfNCR = ncr.ncr_counts().reset_index()

# OSS = On-Street Share (avg % of on-street charging demand, inverse of "home-charging" demand)
oss = 0.32 #use FieldDynamics national average - mention field dynamics full map but how its data was not accessible
oss_lbo = 0.59



# taking a frame of the DF that includes LAs in the CAU
for df in DF:
    df_CAU = df[df['ONSCode'].isin(CAU_LAs)]
    
    cols = list(df_CAU.columns)
    s_row = pd.Series([ONSCode,local_authority_name,year,df_CAU[cols[3]].sum(),ONSGeo,DATE,df_CAU[cols[6]].sum(),df_CAU[cols[7]][0],df_CAU[cols[8]].sum(),df_CAU[cols[9]][0],100*(df_CAU[cols[6]].sum()/df_CAU[cols[8]].sum()),df_CAU[cols[11]][0].sum(),df_CAU[cols[12]][0],df_CAU[cols[13]].sum(),df_CAU[cols[14]][0],100*(df_CAU[cols[11]].sum()/df_CAU[cols[13]].sum()),df_CAU[cols[16]].sum(),df_CAU[cols[17]][0],df_CAU[cols[18]].sum(),df_CAU[cols[19]][0],100*(df_CAU[cols[16]].sum()/df_CAU[cols[18]].sum()),df_CAU[cols[21]].sum(),100*(df_CAU[cols[21]].sum()/df_CAU[cols[3]].sum()),df_CAU[cols[21]].sum()*em.kwh_mi/1000], index = df.columns)
    
    df_CAU = df_CAU.append(s_row,ignore_index=True)

    df = pd.concat([df,df_CAU[df_CAU['ONSCode'] == ONSCode]], ignore_index = True)
    
    df = pd.merge(df, dfNCR, left_on = 'ONSCode', right_on = 'CTYCODE')

    dfComp = ncr.NCR_competition_summary()

    df = pd.merge(df,dfComp,left_on = 'ONSCode', right_on = 'CTYCODE')

    #below replaced with exact oss for LBOs and national avg for rest
    #conditions = [df[df['ONSCode']].isin(LBOs)]
    #choices = [oss_lbo]
    #df['OSS'] = np.select(conditions, choices, default = oss)


    dfOSS = pd.read_csv('Results/Geography/LBO_OSS.csv')

    df = pd.merge(df,dfOSS[['Census_Code','OSS']], left_on = 'ONSCode', right_on = 'Census_Code', how = 'left').fillna(oss)
    df = df.drop(['Census_Code'], axis = 1)

    df['CSUtotal'] = df['OSS']*df['Dem_MWh']/(df['TOTALOUTPUTKW']*8760/1000)
    df['CSUhighest'] = df['OSS']*df['Dem_MWh']/(df['Highest_kW']*8760/1000)

    ORCS = orcs.load_ORCS()
    IMD = imd.load_IMD()

    df = pd.merge(df,ORCS,left_on = 'ONSCode', right_on = 'ONSCode', how = 'left')

    DFcon.append(df)
    

(dfCars_min, dfCars_max, dfTotal_min, dfTotal_max) = (DFcon[0],DFcon[1],DFcon[2],DFcon[3])

dfCars_min.to_csv('Results/SupplyDemand/Cars_min.csv', index=False)
dfCars_max.to_csv('Results/SupplyDemand/Cars_max.csv', index=False)