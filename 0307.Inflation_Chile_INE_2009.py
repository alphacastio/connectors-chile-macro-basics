#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
from datetime import datetime
import requests
import numpy as np
import re

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

df = pd.read_excel("https://ine.cl/docs/default-source/%C3%ADndice-de-precios-al-consumidor/cuadros-estadisticos/series-empalmadas-y-antecedentes-historicos/series-empalmadas-diciembre-2009-a-la-fecha/anal%C3%ADticos-empalmados-base-2018-xls.xlsx?sfvrsn=b0dd286_48", skiprows=4, sheet_name='Analiticos Base 2018=100')

df = df[df.columns.drop(list(df.filter(regex='Variación')))]

df['month'] = df['mes']
df['year'] = df['Año']
df['day'] = 1
df['Date'] = pd.to_datetime(df[['year', 'month', 'day']])
df.drop(['year', 'month', 'day', "Año", "mes"], axis=1, inplace=True)

df = df.set_index(['Date','Glosa'])
df= df.unstack('Glosa')

if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.map(' - '.join)
    for col in df.columns:        
        df =  df.rename(columns= {col: col.replace("Índice - ", "")})
df['country'] = 'Chile'


alphacast.datasets.dataset(307).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
