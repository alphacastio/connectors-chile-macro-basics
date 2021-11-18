#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
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


#Esto permite traer los 2 ultimos años
parametros = f'cbFechaInicio={start_year}&cbFechaTermino={final_year}&cbFrecuencia=QUARTERLY&cbCalculo=NONE&cbFechaBase='

# parametros = 'cbFechaInicio=2003&cbFechaTermino=2021&cbFrecuencia=QUARTERLY&cbCalculo=NONE&cbFechaBase='

#Se descarga la página con los cuadros
activos = requests.get('https://si3.bcentral.cl/Siete/ES/Siete/Cuadro/CAP_BDP/MN_BDP42/'
                    'BP6M_CF_ACT/BP6M_CF_ACT?' + parametros)

pasivos = requests.get('https://si3.bcentral.cl/Siete/ES/Siete/Cuadro/CAP_BDP/MN_BDP42/' 
                       'BP6M_CF_PAS/BP6M_CF_PAS?' + parametros)
                       


# In[5]:


#Hacemos la lectura de las paginas para extraer los datos
df_activos = pd.read_html(activos.content, thousands='.', decimal=',')[0]
df_pasivos = pd.read_html(pasivos.content, thousands='.', decimal=',')[0]


# In[6]:


#Se eliminan las columnas de seleccion de cada item
df_activos.drop('Sel.', axis=1, inplace=True)
df_pasivos.drop('Sel.', axis=1, inplace=True)


# In[7]:


#Como en la tabla no se puede identificar anidamiento, se hace por reemplazo de cada item

dict_activos = {'Cuenta financiera total, activos':'Activos', 
                'Inversión directa activos':'Activos - Inversión directa activos',
                'Participaciones en el capital':'Activos - Inversión directa activos - Participaciones en el capital', 
                'Utilidades reinvertidas':'Activos - Inversión directa activos - Utilidades reinvertidas',
                'Instrumentos de deuda':'Activos - Inversión directa activos - Instrumentos de deuda',
                'Inversión de cartera activos':'Activos - Inversión de cartera activos',
                'Títulos de participaciones en el capital':'Activos - Inversión de cartera activos - Títulos de participaciones en el capital',
                'Títulos de deuda':'Activos - Inversión de cartera activos - Títulos de deuda',
                'Largo plazo':'Activos - Inversión de cartera activos - Títulos de deuda - Largo plazo', 
                'Corto plazo':'Activos - Inversión de cartera activos - Títulos de deuda - Corto plazo', 
                'Instrumentos financieros derivados':'Activos - Instrumentos financieros derivados',
                'Otra inversión':'Activos - Otra inversión', 
                'Créditos comerciales':'Activos - Otra inversión - Créditos comerciales', 
                'Préstamos':'Activos - Otra inversión - Préstamos',
                'Moneda y depósitos':'Activos - Otra inversión - Moneda y depósitos',
                'Otros activos':'Activos - Otra inversión - Otros activos',
                'Activos de reserva':'Activos - Activos de reserva'}

df_activos['Serie'] = df_activos['Serie'].replace(dict_activos, regex=True)


# In[8]:


#Se traspone la matriz y se reemplazan los simbolos de los trimestres por la fecha correspondiente
df_activos = df_activos.set_index('Serie').T

dict_quarters={'IV.':'10-01-', 'III.':'07-01-', 'II.':'04-01-', 'I.':'01-01-'}

df_activos.index = df_activos.index.to_series().replace(dict_quarters, regex=True)
df_activos.index = pd.to_datetime(df_activos.index, format = '%m-%d-%Y')


# In[9]:


#Se hace lo mismo que en el caso de activos. En este caso, cambian un poco los items
#Tener en cuenta que Títulos esta con tilde en activos y sin tilde en pasivos

dict_pasivos = {'Cuenta financiera total, pasivos':'Pasivos', 
                'Inversión directa pasivos':'Pasivos - Inversión directa pasivos',
                'Participaciones en el capital':'Pasivos - Inversión directa pasivos - Participaciones en el capital', 
                'Utilidades reinvertidas':'Pasivos - Inversión directa pasivos - Utilidades reinvertidas',
                'Instrumentos de deuda':'Pasivos - Inversión directa pasivos - Instrumentos de deuda',
                'Inversión de cartera pasivos':'Pasivos - Inversión de cartera pasivos',
                'Titulos de participaciones en el capital':'Pasivos - Inversión de cartera pasivos - Títulos de participaciones en el capital',
                'Titulos de deuda':'Pasivos - Inversión de cartera pasivos - Títulos de deuda',
                'Largo plazo':'Pasivos - Inversión de cartera pasivos - Títulos de deuda - Largo plazo', 
                'Corto plazo':'Pasivos - Inversión de cartera pasivos - Títulos de deuda - Corto plazo', 
                'Instrumentos financieros derivados':'Pasivos - Instrumentos financieros derivados',
                'Otra inversión':'Pasivos - Otra inversión', 
                'Créditos comerciales':'Pasivos - Otra inversión - Créditos comerciales', 
                'Préstamos':'Pasivos - Otra inversión - Préstamos',
                'Moneda y depósitos':'Pasivos - Otra inversión - Moneda y depósitos',
                'Otros pasivos':'Pasivos - Otra inversión - Otros pasivos',
                'Asignaciones DEG':'Pasivos - Otra inversión - Asignaciones DEG'}

df_pasivos['Serie'] = df_pasivos['Serie'].replace(dict_pasivos, regex=True)


# In[10]:


df_pasivos = df_pasivos.set_index('Serie').T

dict_quarters={'IV.':'10-01-', 'III.':'07-01-', 'II.':'04-01-', 'I.':'01-01-'}

df_pasivos.index = df_pasivos.index.to_series().replace(dict_quarters, regex=True)
df_pasivos.index = pd.to_datetime(df_pasivos.index, format = '%m-%d-%Y')


# In[11]:


#Se hace una fusión de ambos dataframes
df = df_activos.merge(df_pasivos, left_index=True, right_index=True, how='outer')


# In[12]:


#Se renombra el eje, índice y se agrega el país
df.rename_axis(None, axis=1, inplace=True)
df.index.names=['Date']
df['country'] = 'Chile'


# In[13]:


# dataset_name = 'BOP - Chile - BCCh - Financial account by categories'

# #Para raw data
# dataset = alphacast.datasets.create(dataset_name, 965, 0)

# alphacast.datasets.dataset(dataset['id']).initialize_columns(dateColumnName = 'Date', 
#             entitiesColumnNames=['country'], dateFormat= '%Y-%m-%d')


# In[14]:


alphacast.datasets.dataset(8325).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




