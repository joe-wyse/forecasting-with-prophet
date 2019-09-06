# -*- coding: utf-8 -*-
"""
Created on Sun May 20 07:49:38 2018

@author: JWeiz
"""



import pandas as pd
import os
import numpy as np
from fbprophet import Prophet
from fbprophet.diagnostics import cross_validation

os.chdir('C:\Users\jweiz\Python Working folder\prophet')
file = '852 DC_SKU DATA.xlsx'

#this section uses a pivot function to make the demand vertical
xl = pd.ExcelFile(file)
rawdf = xl.parse(sheet_name='852',header=0,skiprows=0, index_col=None)
pivotdf = pd.pivot_table(rawdf, index=rawdf.date, columns=rawdf.DC_SKU,
                    values='CODE-OQ', aggfunc='sum')
pivotdf['ds'] = pivotdf.index

#load holidays
holidayfile = 'prophet holidays.xlsx'
holidayxl = pd.ExcelFile(holidayfile)
holidays = holidayxl.parse(sheet_name='Sheet1',header=0,skiprows=0, index_col=0)

#load the previously created demand history
writer = pd.ExcelWriter('All Kroger Demand.xlsx', engine='xlsxwriter')
pivotdf.to_excel(writer, sheet_name='Master', index=True)
pricefile = 'Kroger ads - price manually leaded one day.xlsm'
pricexl = pd.ExcelFile(pricefile)
pricedf = pricexl.parse(sheet_name='AD TIME SERIES', header=0,skiprows=0,index_col=0)


#clear previous dataframes (just in case)
df=None
pdf=None
df=pd.DataFrame()
pdf=pd.DataFrame()
m=None
allforecasts = None
allforecasts = pd.DataFrame()
forecasts = None
forecasts=pd.DataFrame()
forecasts_upper = None
forecasts_upper = pd.DataFrame()
forecasts_lower = None
forecasts_lower = pd.DataFrame()
error = None
error=pd.DataFrame()
percerr=None
percerr=pd.DataFrame()
ontarget=None
ontarget=pd.DataFrame()

#eventually, replace this list with some kind of import from a file
'''
varlist = ('Atlanta_D10VVEG', 'Atlanta_D1214SW', 'Atlanta_D128SHD', 'Atlanta_D610AHW', 'Atlanta_D6AMER', 'Atlanta_D6BUTB', 'Atlanta_D6CAESR', 
'Atlanta_D6CPBBQ', 'Atlanta_D6CPBCN', 'Atlanta_D6CPCHI', 'Atlanta_D6CPCSR', 'Atlanta_D6CSPIN', 'Atlanta_D6ENSMR', 'Atlanta_D6HRTS',
'Atlanta_D6LFCSR', 'Atlanta_D6OAPLD', 'Atlanta_D6OCSAR', 'Atlanta_D6SPCAB', 'Atlanta_D6SWEST', 'Atlanta_D6ULCSR', 'Atlanta_D95ROM',
'Atlanta_DASICHP', 'Atlanta_DFAMUCR', 'Atlanta_DFSACHP', 'Atlanta_DFSSCC', 'Atlanta_DPOMCP', 'Atlanta_DPPSCP', 'Atlanta_DROMFAM',
'Atlanta_DSUNKAL', 'Atlanta_KR65050', 'Atlanta_KR6SM5', 'Atlanta_KR6SP6', 'Atlanta_KRAMR12', 'Atlanta_KRFLDG8', 'Atlanta_KRGRD12',
'Atlanta_KRGRD24', 'Atlanta_KRHRT10', 'Atlanta_KRITL10', 'Atlanta_KRO5016', 'Atlanta_KRO5050', 'Atlanta_KROARU', 'Atlanta_KROKALE',
'Atlanta_KROPWRG', 'Atlanta_KROROM', 'Atlanta_KROSM16', 'Atlanta_KROSM5', 'Atlanta_KROSP16', 'Atlanta_KROSP5', 'Atlanta_KRPRM10',
'Atlanta_KRROM10', 'Atlanta_KRSHRD8', 'Atlanta_KRSLW16', 'Atlanta_KRSPN10', 'Atlanta_KRSPR10', 'Atlanta_KRVEG12', 'Cincinnati_D10VVEG',
'Cincinnati_D1214SW', 'Cincinnati_D128SHD', 'Cincinnati_D610AHW', 'Cincinnati_D610RED', 'Cincinnati_D6AMER',
'Cincinnati_D6BUTB', 'Cincinnati_D6CPBBQ', 'Cincinnati_D6CPBCN', 'Cincinnati_D6CPCHI', 'Cincinnati_D6CPCSR',
'Cincinnati_D6CSPIN', 'Cincinnati_D6ENSMR', 'Cincinnati_D6HRTS', 'Cincinnati_D6LFCSR', 'Cincinnati_D6OAPLD',
'Cincinnati_D6OCSAR', 'Cincinnati_D6SPCAB', 'Cincinnati_D6SWEST', 'Cincinnati_D6ULCSR', 'Cincinnati_D95ROM',
'Cincinnati_DASICHP', 'Cincinnati_DCAESAR', 'Cincinnati_DFAMUCR', 'Cincinnati_DFSACHP', 'Cincinnati_DFSSCC',
'Cincinnati_DPOMCP', 'Cincinnati_DPPSCP', 'Cincinnati_DSUNKAL', 'Cincinnati_KR65050', 'Cincinnati_KR6SM5',
'Cincinnati_KR6SP6', 'Cincinnati_KRAMR12', 'Cincinnati_KRFLDG8', 'Cincinnati_KRGRD12', 'Cincinnati_KRGRD24',
'Cincinnati_KRHRT10', 'Cincinnati_KRITL10', 'Cincinnati_KRO5016', 'Cincinnati_KRO5050', 'Cincinnati_KROARU',
'Cincinnati_KROKALE', 'Cincinnati_KROPWRG', 'Cincinnati_KROROM', 'Cincinnati_KROSM16', 'Cincinnati_KROSM5',
'Cincinnati_KROSP16', 'Cincinnati_KROSP5', 'Cincinnati_KRPRM10', 'Cincinnati_KRROM10', 'Cincinnati_KRSHRD8',
'Cincinnati_KRSLW16', 'Cincinnati_KRSPN10', 'Cincinnati_KRSPR10', 'Cincinnati_KRVEG12', 'Dallas_D10VVEG',
'Dallas_D1214SW', 'Dallas_D128SHD', 'Dallas_D610AHW', 'Dallas_D616SHD', 'Dallas_D6AMER', 'Dallas_D6BUTB', 'Dallas_D6CAESR',
'Dallas_D6CPBBQ', 'Dallas_D6CPBCN', 'Dallas_D6CPCHI', 'Dallas_D6CPCSR', 'Dallas_D6CSPIN', 'Dallas_D6ENSMR', 'Dallas_D6HRTS',
'Dallas_D6LFCSR', 'Dallas_D6OADSK', 'Dallas_D6OAPLD', 'Dallas_D6OCSAR', 'Dallas_D6OCSR', 'Dallas_D6SLKIT', 'Dallas_D6SPCAB',
'Dallas_D6SWEST', 'Dallas_D6ULCSR', 'Dallas_D95ROM', 'Dallas_DASICHP', 'Dallas_DFAMUCR', 'Dallas_DFSACHP', 'Dallas_DFSSCC',
'Dallas_DOKLMX', 'Dallas_DOSRSP', 'Dallas_DPOMCP', 'Dallas_DPPSCP', 'Dallas_DSUNKAL', 'Delaware_D10VVEG',
'Delaware_D1214SW', 'Delaware_D128SHD', 'Delaware_D12SPIN', 'Delaware_D610AHW', 'Delaware_D616SHD',
'Delaware_D6AMER', 'Delaware_D6BUTB', 'Delaware_D6CPBBQ', 'Delaware_D6CPBCN', 'Delaware_D6CPCHI',
'Delaware_D6CPCSR', 'Delaware_D6CSPIN', 'Delaware_D6ENSMR', 'Delaware_D6HRTS', 'Delaware_D6LFCSR',
'Delaware_D6OAPLD', 'Delaware_D6OCSAR', 'Delaware_D6SPCAB', 'Delaware_D6SWEST', 'Delaware_D6ULCSR',
'Delaware_D95ROM', 'Delaware_DASICHP', 'Delaware_DCAESAR', 'Delaware_DFAMUCR', 'Delaware_DFSACHP',
'Delaware_DFSSCC', 'Delaware_DPOMCP', 'Delaware_DPPSCP', 'Delaware_DSUNKAL', 'Delaware_KR65050', 'Delaware_KR6SM5',
'Delaware_KR6SP6', 'Delaware_KRAMR12', 'Delaware_KRFLDG8', 'Delaware_KRGRD12', 'Delaware_KRGRD24',
'Delaware_KRHRT10', 'Delaware_KRITL10', 'Delaware_KRO5016', 'Delaware_KRO5050', 'Delaware_KROARU',
'Delaware_KROKALE', 'Delaware_KROPWRG', 'Delaware_KROROM', 'Delaware_KROSM16', 'Delaware_KROSM5',
'Delaware_KROSP16', 'Delaware_KROSP5', 'Delaware_KRPRM10', 'Delaware_KRROM10', 'Delaware_KRSHRD8',
'Delaware_KRSLW16', 'Delaware_KRSPN10', 'Delaware_KRSPR10', 'Delaware_KRVEG12', 'Houston_D10VVEG',
'Houston_D1214SW', 'Houston_D128SHD', 'Houston_D610AHW', 'Houston_D6AMER', 'Houston_D6BUTB', 'Houston_D6CAESR', 'Houston_D6CPBBQ',
'Houston_D6CPBCN', 'Houston_D6CPCHI', 'Houston_D6CPCSR', 'Houston_D6CSPIN', 'Houston_D6ENSMR', 'Houston_D6HRTS', 'Houston_D6LFCSR',
'Houston_D6OAPLD', 'Houston_D6OCSAR', 'Houston_D6SLKIT', 'Houston_D6SPCAB', 'Houston_D6SWEST', 'Houston_D6ULCSR', 'Houston_D95ROM',
'Houston_DASICHP', 'Houston_DFAMUCR', 'Houston_DFSACHP', 'Houston_DFSSCC', 'Houston_DPOMCP', 'Houston_DPPSCP', 'Houston_DSUNKAL',
'Louisville_D10VVEG', 'Louisville_D1214SW', 'Louisville_D128SHD', 'Louisville_D610AHW', 'Louisville_D616SHD',
'Louisville_D6AMER', 'Louisville_D6BUTB', 'Louisville_D6CAESR', 'Louisville_D6CPBBQ', 'Louisville_D6CPBCN',
'Louisville_D6CPCHI', 'Louisville_D6CPCSR', 'Louisville_D6CSPIN', 'Louisville_D6ENSMR', 'Louisville_D6HRTS',
'Louisville_D6LFCSR', 'Louisville_D6OAPLD', 'Louisville_D6OCSAR', 'Louisville_D6SPCAB', 'Louisville_D6SWEST',
'Louisville_D6ULCSR', 'Louisville_D95ROM', 'Louisville_DASICHP', 'Louisville_DFAMUCR', 'Louisville_DFSACHP',
'Louisville_DFSSCC', 'Louisville_DPOMCP', 'Louisville_DPPSCP', 'Louisville_DSUNKAL', 'Louisville_KR65050',
'Louisville_KR6SM5', 'Louisville_KR6SP6', 'Louisville_KRAMR12', 'Louisville_KRFLDG8', 'Louisville_KRGRD12',
'Louisville_KRGRD24', 'Louisville_KRHRT10', 'Louisville_KRITL10', 'Louisville_KRO5016', 'Louisville_KRO5050',
'Louisville_KROARU', 'Louisville_KROKALE', 'Louisville_KROPWRG', 'Louisville_KROROM', 'Louisville_KROSM16',
'Louisville_KROSM5', 'Louisville_KROSP16', 'Louisville_KROSP5', 'Louisville_KRPRM10', 'Louisville_KRROM10',
'Louisville_KRSHRD8', 'Louisville_KRSLW16', 'Louisville_KRSPN10', 'Louisville_KRSPR10', 'Louisville_KRVEG12',
'Salem_D10VVEG', 'Salem_D1214SW', 'Salem_D128SHD', 'Salem_D610AHW', 'Salem_D6AMER', 'Salem_D6BUTB', 'Salem_D6CAESR',
'Salem_D6CPBBQ', 'Salem_D6CPBCN', 'Salem_D6CPCHI', 'Salem_D6CPCSR', 'Salem_D6CSPIN', 'Salem_D6ENSMR', 'Salem_D6HRTS',
'Salem_D6LFCSR', 'Salem_D6OAPLD', 'Salem_D6OCSAR', 'Salem_D6SLKIT', 'Salem_D6SPCAB', 'Salem_D6SWEST', 'Salem_D6ULCSR',
'Salem_D95ROM', 'Salem_DASICHP', 'Salem_DFAMUCR', 'Salem_DFSACHP', 'Salem_DFSSCC', 'Salem_DPOMCP', 'Salem_DPPSCP',
'Salem_DSUNKAL', 'Salem_KR65050', 'Salem_KR6SM5', 'Salem_KR6SP6', 'Salem_KRAMR12', 'Salem_KRFLDG8', 'Salem_KRGRD12',
'Salem_KRGRD24', 'Salem_KRHRT10', 'Salem_KRITL10', 'Salem_KRO5016', 'Salem_KRO5050', 'Salem_KROARU', 'Salem_KROKALE',
'Salem_KROPWRG', 'Salem_KROROM', 'Salem_KROSM16', 'Salem_KROSM5', 'Salem_KROSP16', 'Salem_KROSP5', 'Salem_KRPRM10',
'Salem_KRROM10', 'Salem_KRSHRD8', 'Salem_KRSLW16', 'Salem_KRSPN10', 'Salem_KRSPR10', 'Salem_KRVEG12')
'''

varlist = ('Atlanta_D10VVEG', 'Atlanta_D1214SW')


#varlist2 = ("Salem_KRVEG12")

for var in varlist:
    print(var)
    df=pivotdf[['ds',var]]
    df=df.rename(index=str, columns={var:"y"})
    df['y'] = df['y'] + 1
    df['y'] = df['y'].apply(np.log)

    #m = Prophet(holidays=holidays)
    pvar = var
    if pvar in pricedf.columns:
        pdf = pricedf[['ds', pvar]]
        pdf=pdf.rename(index=str,columns={pvar:"price"})
        pdf = pdf[['ds', 'price']]
        pdf['price'] = pdf['price'].apply(np.log)
        df = df.merge(pdf,how='left', on='ds', left_on=None, right_on=None, left_index=False, sort=True)
        try:
            m = Prophet(holidays=holidays)
            m.add_regressor('price')
            m.fit(df)
            prefuture = m.make_future_dataframe(periods=365)
            future = pd.merge(prefuture, pdf[['ds', 'price']], how='inner', on='ds', left_on=None, right_on=None,
                              left_index=False, right_index=False, sort=True,
                              suffixes=('_x', '_y'), copy=True, indicator=False,
                              validate=None)
            forecast = m.predict(future)
            print("Fit model with price var")
            allforecasts[var] = pow(2.71828,forecast['yhat'])
            allforecasts['ds'] = forecast['ds']
        except:
            print("Exception: found price var, couldn't fit")
            pass
    else:
        try:
            m = Prophet(holidays=holidays)
            m.fit(df)
            future = m.make_future_dataframe(periods=28)
            print("Fit model without price var")
            forecast = m.predict(future)
            #try adding + 1 to the line below
            allforecasts[[var]] = pow(2.71828,forecast['yhat'])
            allforecasts[['ds']] = forecast[['ds']]
        except:
            print("Couldn't fit model even without price var")
            pass
writer = pd.ExcelWriter('logged y and price prophet forecasts.xlsx', engine='xlsxwriter')
allforecasts.to_excel(writer, sheet_name='fc', index=True)


    
    
    