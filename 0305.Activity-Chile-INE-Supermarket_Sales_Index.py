#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
from datetime import datetime
import requests
import numpy as np
import re

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


df = pd.read_excel("https://www.ine.cl/docs/default-source/ventas-de-supermercados/cuadros-estadisticos/base-promedio-a%C3%B1o-2014-100/series-mensuales-desde-enero-2014-a-la-fecha.xls?sfvrsn=8490e4d6_62", skiprows=6, sheet_name='1')

df = df.dropna(how='all', subset=df.columns[1:])

df = df.loc[:, ~(df == '/R').any()]

df = df[df.columns.drop(list(df.filter(regex='Variación')))]

def dateWrangler(x):
    global y
    x=str(x)
    list_x = x.split('-')
    if list_x[0] == 'ene':
        y = '20' + list_x[1]+'-01-01'
    elif list_x[0] == 'feb':
        y = '20' + list_x[1]+'-02-01'
    elif list_x[0] == 'mar':
        y = '20' + list_x[1]+'-03-01'
    elif list_x[0] == 'abr':
        y = '20' + list_x[1]+'-04-01'
    elif list_x[0] == 'may':
        y = '20' + list_x[1]+'-05-01'
    elif list_x[0] == 'jun':
        y = '20' + list_x[1]+'-06-01'
    elif list_x[0] == 'jul':
        y = '20' + list_x[1]+'-07-01'
    elif list_x[0] == 'ago':
        y = '20' + list_x[1]+'-08-01'
    elif list_x[0] == 'sep':
        y = '20' + list_x[1]+'-09-01'    
    elif list_x[0] == 'oct':
        y = '20' + list_x[1]+'-10-01'
    elif list_x[0] == 'nov':
        y = '20' + list_x[1]+'-11-01'
    elif list_x[0] == 'dic':
        y = '20' + list_x[1]+'-12-01'    
    return y

df['Mes y año'] = df['Mes y año'].apply(lambda x: dateWrangler(x))

for col in df.columns:
        new_col = re.sub(' +', ' ', col).replace(" /Ref/P/R", "")
        df = df.rename(columns={col: new_col})        

df = df.rename({'Mes y año': 'Date'}, axis=1)        

df['Date']=pd.to_datetime(df['Date'])

df = df.set_index('Date')

df


# In[6]:


df1 = pd.read_excel("https://www.ine.cl/docs/default-source/ventas-de-supermercados/cuadros-estadisticos/base-promedio-a%C3%B1o-2014-100/serie-mensual-empalmada-desestacionalizada-y-tendencia-ciclo-desde-1991-hasta-la-fecha.xls?sfvrsn=a4f5a9d6_58", skiprows=5, sheet_name='ISUP, serie empalmada')

df1 = df1.dropna(how = 'all').dropna(how = 'all',axis=1)

df1 = df1.dropna(how='all', subset=df1.columns[1:])

df1 = df1.loc[:,~(df1=='/R').any()] 

df1 = df1[df1.columns.drop(list(df1.filter(regex='Variación')))]

for col in df1.columns:
        new_col = re.sub(' +', ' ', col).replace("1 /Ref/P/R", "").replace("*/**/***", "")
        df1 = df1.rename(columns={col: new_col})   

df1 = df1.rename({'Mes y año': 'Date'}, axis=1)          

df1['Date']=pd.to_datetime(df1['Date'])

df1 = df1.set_index('Date')
    

dfFinal = df.merge(df1, how='outer', left_index=True, right_index=True)
dfFinal['country'] = 'Chile'

alphacast.datasets.dataset(305).upload_data_from_df(dfFinal, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
