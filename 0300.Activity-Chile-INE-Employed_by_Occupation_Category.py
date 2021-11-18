#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from datetime import datetime
import requests
import numpy as np
import re

import re
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


##Revisar cuando carga la pagina https://www.ine.cl/estadisticas/sociales/mercado-laboral/ocupacion-y-desocupacion
#Se va a Cuadros estadisticos y luego a Series de Tiempo de 2017, ahi muestra el listado de archivos

#Con la primera solicitud se obtiene el link de la carpeta
post_data = {'idFolder': '0aa7885f-6806-4ddc-a6b5-77bd3b2e0257'}

carpeta = requests.post('https://www.ine.cl/estadisticas/sociales/mercado-laboral/ocupacion-y-desocupacion/hijosCarpeta/',
              data=post_data)


# In[3]:


#Se obtiene el Id para luego poder hacer un segundo request y determinar el link del archivo
null=None
carpeta_id = pd.DataFrame(eval(carpeta.content.decode('utf-8'))['folder'])['Id'][0]


# In[4]:


#En el segundo requests.post se trae el listado de archivos con su url
post_data_files = {'idFolder': carpeta_id}

files = requests.post('https://www.ine.cl/estadisticas/sociales/mercado-laboral/ocupacion-y-desocupacion/getArchivos/',
              data=post_data_files)


# In[5]:


#Se convierte el json a un dataframe para poder filtrarlo
df_files = pd.DataFrame(eval(files.content.decode('utf-8'))['documento'])
#Se convierte a list porque al filtrar por la celda, extrae un link recortado
link_xls = list(df_files[df_files['Titulo'] == 'Serie: Personas ocupadas según categoría en la ocupación']['Url'])


# In[6]:


#Se descarga el archivo
response = requests.get(link_xls[0])


# In[7]:


loop = {
    1:{"sheet":'as', "prefix":'Ambos Sexos'},
    2:{"sheet":'h', "prefix":'Hombre'}, 
    3:{"sheet":'m', "prefix":'Mujer'} 
}

dfFinal = pd.DataFrame()        


# In[8]:


for key in loop.keys():     
    df = pd.read_excel(response.content, skiprows=5, sheet_name=loop[key]["sheet"], header=[0,1])
    
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.map(' - '.join)

    for col in df.columns:
        new_col = re.sub(' +', ' ', col).replace(" - nota", "").replace(" - Unnamed: 0_level_1", "").replace("- Unnamed: 1_level_1","").replace(" /2","").replace(" /3","").replace(" /4","").replace(" /5","").replace(" /6","")
        df = df.rename(columns={col: new_col})

    df = df.dropna(how = 'all').dropna(how = 'all',axis=1)

    df = df.loc[:,~(df=='a').any()]    
    df = df.loc[:,~(df=='b').any()] 
    df = df.dropna(how='all', subset=df.columns[1:])

    df['Trimestre '] = df['Trimestre '].astype(str)

    df['Trimestre '] = df['Trimestre '].replace(
        {
            "Ene - Mar": "02-01",
            "Feb - Abr": "03-01",
            "Mar - May": "04-01",
            "Abr - Jun": "05-01",
            "May -Jul":  "06-01",
            "Jun - Ago": "07-01",
            "Jul - Sep": "08-01",
            "Ago - Oct": "09-01",
            "Sep - Nov": "10-01",
            "Oct - Dic": "11-01",
            "Nov - Ene": "12-01",
            "Dic - Feb": "01-01",
        })

    df['Año'] = df['Año'].astype(str)

    df['Date'] = df['Año'] + '-' + df['Trimestre ']

    del df['Año']
    del df['Trimestre '] 

    df['Date']=pd.to_datetime(df['Date'])

    df = df.set_index('Date')
    
    for col in df.columns:
        df = df.rename(columns={col: loop[key]["prefix"] + ' - ' + col})
    
    dfFinal = dfFinal.merge(df, how='outer', left_index=True, right_index=True)


# In[9]:


dfFinal['country'] = 'Chile'


# In[10]:


alphacast.datasets.dataset(300).upload_data_from_df(dfFinal, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


