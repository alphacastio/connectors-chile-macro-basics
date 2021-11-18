#!/usr/bin/env python
# coding: utf-8

# In[24]:


# !pip install lxml 
import pandas as pd
import requests, json
import numpy as np
# from urllib.request import urlopen
from lxml import etree
from collections import OrderedDict

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[25]:


origin_url = "https://www.ine.cl/estadisticas/economia/comercio-servicios-y-turismo/actividad-mensual-del-comercio"
response = requests.get(origin_url)

html = response.content

htmlparser = etree.HTMLParser()
tree = etree.fromstring(html, htmlparser)
idFolder = tree.xpath("//div[@class='navPrincipalDescargas']/@data-folder")
idFolder = idFolder[0]
idFolder


# In[26]:


# get folder list
url = "https://www.ine.cl/estadisticas/economia/energia-y-medioambiente/produccion-de-electricidad-gas-y-agua/hijosCarpeta/"

payload={'idFolder': idFolder}
headers = {
  'Cookie': 'BIGipServerpool_portal_ine_cl=!9egTAeaJ5qOeALxg1ox3tfrS5n82lbC08DQyRQhBHCLb9Ta3U27VDE60o9PMk51fF8tQeoC4lRSEnQ=='
}

response = requests.request("POST", url, headers=headers, data=payload)
folders_txt = response.text

folders_txt


# In[27]:


# get product list
json_data = json.loads(folders_txt)
folder = json_data['folder'][0]
folder_id = folder['Id']
folder_id


# In[28]:


url_for_folder = "https://www.ine.cl/estadisticas/economia/comercio-servicios-y-turismo/actividad-mensual-del-comercio/getArchivos/"

payload={'idFolder': folder_id}
headers = {
  'Cookie': 'BIGipServerpool_portal_ine_cl=!9egTAeaJ5qOeALxg1ox3tfrS5n82lbC08DQyRQhBHCLb9Ta3U27VDE60o9PMk51fF8tQeoC4lRSEnQ=='
}

response_for_folder = requests.request("POST", url_for_folder, headers=headers, data=payload)
folder_txt = response_for_folder.text

folder_txt


# In[29]:


json_data_for_folder = json.loads(folder_txt)
documento = json_data_for_folder['documento'][1]
documento_url = documento['Url']
documento_url


# In[30]:


payload={}
headers = {}

response = requests.request("GET", documento_url, headers=headers, data=payload)
filename = 'excel.xls'
if response.status_code == 200:
    open(filename, 'wb').write(response.content)
response.status_code


# In[31]:


df = pd.read_excel(filename, skiprows= 5, header=[0])
df


# In[32]:


df_drop_first_column = df.drop([df.columns[0], df.columns[3], df.columns[4], df.columns[5], df.columns[7], df.columns[8], df.columns[9], df.columns[11]], axis=1)
df_drop_first_column


# In[33]:


df_drop_rows_nan = df_drop_first_column[df_drop_first_column['Mes y año'].notnull()]
df_drop_rows_nan = df_drop_rows_nan.rename(columns= {'Mes y año':'Date'})
df_drop_rows_nan = df_drop_rows_nan.set_index('Date')
df_drop_rows_nan['country'] = 'Chile'
df_drop_rows_nan.head()


alphacast.datasets.dataset(128).upload_data_from_df(df_drop_rows_nan, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)




