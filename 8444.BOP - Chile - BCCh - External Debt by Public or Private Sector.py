#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import numpy as np
import pandas as pd
from datetime import date
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


# In[3]:


#Defino el periodo de inicio y final
#Para actualizarlo no se trae todo el historico, sino a partir de los 2 años más recientes
final_year = str(date.today().year)
start_year = str(date.today().year - 2)


# In[4]:


#Esto traia la informacion hasta hoy
# params = 'cbFechaInicio=2003&cbFechaTermino=2021&cbFrecuencia=MONTHLY&cbCalculo=NONE&cbFechaBase='

#Con esta especificación, se trae solo los últimos 2 años
params = f'cbFechaInicio={start_year}&cbFechaTermino={final_year}&cbFrecuencia=MONTHLY&cbCalculo=NONE&cbFechaBase='

page = requests.get('https://si3.bcentral.cl/Siete/ES/Siete/Cuadro/CAP_BDP/MN_BDP42/BP6M_DE_PUB_PRV/BP6M_DE_PUB_PRV?' + params)


# In[5]:


df = pd.read_html(page.content, thousands='.', decimal=',')[0]


# In[6]:


df.drop('Sel.', axis=1, inplace=True)
#Se crea una columna para tener el orden de las filas
df['categoria'] = df.reset_index().index


# In[7]:


#Como los nombres de las series se repiten en varios casos, se utiliza el orden en la tabla para hacer el renombramiento

dict_categorias = {0:'Deuda externa total', 1:'Deuda externa total - Sector público',
                   2:'Deuda externa total - Sector público - Gobierno General',
                   3:'Deuda externa total - Sector público - Banco Central',
                   4:'Deuda externa total - Sector público - Bancos', 
                   5:'Deuda externa total - Sector público - Empresas no financieras', 6:'Deuda externa total - Sector privado',
                   7:'Deuda externa total - Sector privado - Bancos',
                   8:'Deuda externa total - Sector privado - Otras sociedades financieras',
                   9:'Deuda externa total - Sector privado - Empresas no financieras', 10:'Deuda Externa de Corto Plazo',
                   11:'Deuda Externa de Corto Plazo - Sector público', 
                   12:'Deuda Externa de Corto Plazo - Sector público - Gobierno General',
                   13:'Deuda Externa de Corto Plazo - Sector público - Banco Central',
                   14:'Deuda Externa de Corto Plazo - Sector público - Bancos',
                   15:'Deuda Externa de Corto Plazo - Sector público - Empresas no financieras',
                   16:'Deuda Externa de Corto Plazo - Sector privado',
                   17:'Deuda Externa de Corto Plazo - Sector privado - Bancos',
                   18:'Deuda Externa de Corto Plazo - Sector privado - Otras sociedades financieras',
                   19:'Deuda Externa de Corto Plazo - Sector privado - Empresas no financieras',
                   20:'Deuda Externa de Largo Plazo', 21:'Deuda Externa de Largo Plazo - Sector público',
                   22:'Deuda Externa de Largo Plazo - Sector público - Gobierno General',
                   23:'Deuda Externa de Largo Plazo - Sector público - Banco Central',
                   24:'Deuda Externa de Largo Plazo - Sector público - Bancos', 
                   25:'Deuda Externa de Largo Plazo - Sector público - Empresas no financieras',
                   26:'Deuda Externa de Largo Plazo - Sector privado', 
                   27:'Deuda Externa de Largo Plazo - Sector privado - Bancos', 
                   28:'Deuda Externa de Largo Plazo - Sector privado - Otras sociedades financieras',
                   29:'Deuda Externa de Largo Plazo - Sector privado - Empresas no financieras'}

#Itero sobre los elementos del diccionario y hago los reemplazos en Serie
for k, v in dict_categorias.items():
    df['Serie'] = np.where(df['categoria'] == k, v, df['Serie'])

#Elimino la columna de categoria antes de trasponer, para asi no queda como una fila adicional
df.drop('categoria', axis=1, inplace=True)


# In[8]:


#Seteo el indice en Serie para poder trasponer
df = df.set_index('Serie').T


# In[9]:


#Hago los reemplazos de los nombres de los meses
dict_months={'Ene':'01', 'Feb':'02', 'Mar':'03', 'Abr':'04', 'May':'05', 'Jun':'06', 'Jul':'07', 'Ago':'08',
              'Sep':'09', 'Oct':'10', 'Nov':'11', 'Dic':'12'}

df.index = df.index.to_series().replace(dict_months, regex=True)
#Cambio a formato fecha
df.index = pd.to_datetime(df.index, format = '%m.%Y')


# In[10]:


#Agrego el pais y renombro el indice
df['country'] = 'Chile'
df.index.names=['Date']
df.rename_axis(None, axis=1, inplace=True)


# In[11]:


# dataset_name = 'BOP - Chile - BCCh - External Debt by Public or Private Sector'

# #Para raw data
# dataset = alphacast.datasets.create(dataset_name, 965, 0)

# alphacast.datasets.dataset(dataset['id']).initialize_columns(dateColumnName = 'Date', 
#             entitiesColumnNames=['country'], dateFormat= '%Y-%m-%d')


# In[12]:


alphacast.datasets.dataset(8444).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




