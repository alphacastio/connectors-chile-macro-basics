#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import pandas as pd
from datetime import date
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


# In[ ]:


#Defino el periodo de inicio y final
#Para actualizarlo no se trae todo el historico, sino a partir de los 2 años más recientes
final_year = str(date.today().year)
start_year = str(date.today().year - 2)


# In[ ]:


###La idea es scrapear el cuadro que sale en la página del Banco Central de Chile

#Esto permite traer los 2 ultimos años
parametros = f'cbFechaInicio={start_year}&cbFechaTermino={final_year}&cbFrecuencia=QUARTERLY&cbCalculo=NONE&cbFechaBase='

# parametros = 'cbFechaInicio=1996&cbFechaTermino=2021&cbFrecuencia=QUARTERLY&cbCalculo=NONE&cbFechaBase='

page = requests.get('https://si3.bcentral.cl/Siete/ES/Siete/Cuadro/CAP_CCNN/MN_CCNN76'
                    '/CCNN2013_G3_RPIB/CCNN2013_G3_RPIB?' + parametros)


# In[ ]:


df = pd.read_html(page.content, thousands='.', decimal=',')[0]


# In[ ]:


df.drop('Sel.', axis=1, inplace=True)
df = df.set_index('Serie').T


# In[ ]:


#Se crea el diccionario y luego se hace el reemplazo. Se podría recorrer diccionario y hacer los reemplazos
#En cada ocurrencia
dict_quarters={'IV.':'10-01-', 'III.':'07-01-', 'II.':'04-01-', 'I.':'01-01-'}

df.index = df.index.to_series().replace(dict_quarters, regex=True)
df.index = pd.to_datetime(df.index, format = '%m-%d-%Y')


# In[ ]:


df.index.names = ['Date']
df.rename_axis(None, axis=1, inplace=True)
df['country'] = 'Chile'


# In[ ]:


# dataset_name = 'Activity - Chile - BCCh - Variation of existences'

# #Para raw data
# dataset = alphacast.datasets.create(dataset_name, 965, 0)

# alphacast.datasets.dataset(dataset['id']).initialize_columns(dateColumnName = 'Date', 
#             entitiesColumnNames=['country'], dateFormat= '%Y-%m-%d')


# In[ ]:


alphacast.datasets.dataset(8301).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




