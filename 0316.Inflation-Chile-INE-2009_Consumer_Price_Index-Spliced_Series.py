#!/usr/bin/env python
# coding: utf-8

# In[9]:


import pandas as pd
import numpy as np
import requests

from datetime import datetime
from urllib.request import urlopen
from lxml import etree
import io
import re
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[10]:


url = "https://ine.cl/docs/default-source/índice-de-precios-al-consumidor/cuadros-estadisticos/series-empalmadas-y-antecedentes-historicos/series-empalmadas-diciembre-2009-a-la-fecha/serie-histórica-empalmada-ipc-diciembre-2009-a-la-fecha-xls.xlsx?sfvrsn=43e00582_49"
r = requests.get(url, allow_redirects=False,verify=False)

df = pd.read_excel(r.content,skiprows = 3, sheet_name = 'Serie ipc empalmada')

df = df.iloc[:,[0,1,2]]
df['month'] = df['Mes']
df['year'] = df['Año']
df['day'] = 1
df['Date'] = pd.to_datetime(df[['year', 'month', 'day']])
df = df.drop(['year', 'month', 'day', "Año", "Mes"], axis=1)
df = df.set_index('Date')
df


# In[11]:


df["country"] = "Chile"

alphacast.datasets.dataset(316).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
