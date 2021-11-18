#!/usr/bin/env python
# coding: utf-8

# 1. Cargar la data (automatico / csv)
# 1b. Carga de la data auxiliar
# 2. Mensualizar o no menzualizar
# 3. Definir las tranformaciones
# 4. Transformar
# 5. Exportar a csv / a la base

# In[11]:


import pandas as pd
import statsmodels.api as sm
import datetime
import numpy as np
import matplotlib

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[3]:


df = pd.read_excel("https://si3.bcentral.cl/estadisticas/Diario1/Excel/EC/Operaciones_mercado_cambiario_formal.xls", 
                           sheet_name="MCF", 
                           skiprows= 4, 
                           header=[0,1])
df.columns = df.columns.map(' - '.join)
    
df.dropna(axis=0, how='all',inplace=True)
df.dropna(axis=1, how='all',inplace=True)    

replacements = {"Marzo":3 ,"marzo":3,"Abril":4,"Mayo":5,"Junio":6,"Julio":7,"julio":7,"Agosto":8,"Septiembre":9,"Octubre":10
,"Noviembre":11,"Diciembre":12,"Enero":1,"febrero":2,"Febrero":2,"Ago": 8,"Sept": 9,"Oct": 10,"Nov": 11,"Dic": 12,"Ene": 1,"Feb": 2
,"Mar": 3,"Abr": 4,"May": 5,"Jun": 6,"Jul": 7,"Sep": 9}

for key in replacements:
    df["Unnamed: 1_level_0 - Mes"] = df["Unnamed: 1_level_0 - Mes"].replace(key,replacements[key])


# In[4]:


#Codigo para arregla la fecha de septiembre de 2020 en la que no aparece el cambio de mes
df["dia_anterior"] = df["Unnamed: 2_level_0 - Día"].shift(1)
df["mes_temp"] = df["Unnamed: 1_level_0 - Mes"].ffill()
df["mes_temp"] = pd.to_numeric(df["mes_temp"], errors='coerce')
df.loc[(df["dia_anterior"]==31) & (df["Unnamed: 2_level_0 - Día"]==1) & (df["Unnamed: 1_level_0 - Mes"].isnull()), "Unnamed: 1_level_0 - Mes"] = df["mes_temp"] + 1 


# pd.set_option('display.max_rows', 5000)
# df[5800:]

# In[8]:


df["Unnamed: 0_level_0 - Año"] = df["Unnamed: 0_level_0 - Año"].ffill()
df["Unnamed: 1_level_0 - Mes"] = df["Unnamed: 1_level_0 - Mes"].ffill()
df["Unnamed: 2_level_0 - Día"] = df["Unnamed: 2_level_0 - Día"].ffill()

df = df[df["Mercado de Derivados Total - Posición de cambios Derivados Total"] != 0]

df["year"] = pd.to_numeric(df["Unnamed: 0_level_0 - Año"], errors='coerce')
df["month"] = pd.to_numeric(df["Unnamed: 1_level_0 - Mes"], errors='coerce')
df["day"] = pd.to_numeric(df["Unnamed: 2_level_0 - Día"], errors='coerce')
df["Date"] = pd.to_datetime(df[["year", "month", "day"]])
df = df[df["Date"].notnull()]
df = df.set_index("Date")
del df["year"]
del df["day"]
del df["month"]
del df["Unnamed: 1_level_0 - Mes"]
del df["Unnamed: 2_level_0 - Día"]
del df["Unnamed: 0_level_0 - Año"]
del df["dia_anterior"]
del df["mes_temp"]


# ## Cargar la data (automatico / csv)

# In[12]:


df["country"] = "Chile"

alphacast.datasets.dataset(6).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
