#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from urllib.request import urlopen

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


url = "https://www.ine.cl/docs/default-source/%C3%ADndice-de-precios-al-consumidor/cuadros-estadisticos/base-2018/series-de-tiempo/ipc-xls.xlsx?sfvrsn=bece7e56_42"
print (url)
r = requests.get(url, allow_redirects=False)
filename = '..//Input//Conector 1//excel.xls'

open(filename, 'wb').write(r.content)


# In[3]:


df = pd.read_excel(filename, skiprows=3)
df = df.rename(columns={"Año": "Year", "Mes": "Month"})
df["Day"] = 1
df["Date"] = pd.to_datetime(df[["Year", "Month", "Day"]])
del df["Year"]
del df["Month"]
del df["Day"]


df["producto"] = (df["División"].fillna("").replace(" ","").astype("str")).str[-2:] + "-" + (df["Grupo"].fillna("").replace(" ","").astype("str")) + "-" + (df["Clase"].fillna("").replace(" ","").astype("str")) + "-" + (df["Subclase"].fillna("").replace(" ","").astype("str")) + "-" + (df["Producto"].fillna("").replace(" ","").astype("str")).str[-2:] + "-" + df["Glosa"]
df = df[["Date", "producto", "Índice"]].set_index(["Date", "producto"])
df = df.unstack(level=1)
if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.map(' - '.join)
for c in df.columns:
    df = df.rename(columns={c: c.replace("Índice -", "").strip()})
df["country"] = "Chile"


alphacast.datasets.dataset(97).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

