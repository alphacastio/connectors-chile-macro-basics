#!/usr/bin/env python
# coding: utf-8

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

# In[2]:


origin_url = "https://www.ine.cl/estadisticas/economia/energia-y-medioambiente/produccion-de-electricidad-gas-y-agua/"
response = requests.get(origin_url)

html = response.content

htmlparser = etree.HTMLParser()
tree = etree.fromstring(html, htmlparser)
idFolder = tree.xpath("//div[@class='navPrincipalDescargas']/@data-folder")
idFolder = idFolder[0]
idFolder


# In[3]:


# get folder list
url = "https://www.ine.cl/estadisticas/economia/energia-y-medioambiente/produccion-de-electricidad-gas-y-agua/hijosCarpeta/"

payload={'idFolder': idFolder}
headers = {
  'Cookie': 'BIGipServerpool_portal_ine_cl=!9egTAeaJ5qOeALxg1ox3tfrS5n82lbC08DQyRQhBHCLb9Ta3U27VDE60o9PMk51fF8tQeoC4lRSEnQ=='
}

response = requests.request("POST", url, headers=headers, data=payload)
folders_txt = response.text

folders_txt


# In[4]:


# get product list
json_data = json.loads(folders_txt)
folder = json_data['folder'][0]
folder_id = folder['Id']
folder_id


# In[5]:


url_for_folder = "https://www.ine.cl/estadisticas/economia/energia-y-medioambiente/produccion-de-electricidad-gas-y-agua/getArchivos/"

payload={'idFolder': folder_id}
headers = {
  'Cookie': 'BIGipServerpool_portal_ine_cl=!9egTAeaJ5qOeALxg1ox3tfrS5n82lbC08DQyRQhBHCLb9Ta3U27VDE60o9PMk51fF8tQeoC4lRSEnQ=='
}

response_for_folder = requests.request("POST", url_for_folder, headers=headers, data=payload)
folder_txt = response_for_folder.text

folder_txt


# In[6]:


json_data_for_folder = json.loads(folder_txt)
documento = json_data_for_folder['documento'][0]
documento_url = documento['Url']
documento_url


# In[7]:


df = pd.read_excel(documento_url, sheet_name='1', skiprows=6,header=[0])
df


# In[8]:


df_drop_first_column = df.drop([df.columns[0], df.columns[3], df.columns[4], df.columns[5], df.columns[7], df.columns[8], df.columns[9], df.columns[11]], axis=1)
df_drop_first_column


# In[9]:


df_drop_rows_nan = df_drop_first_column[df_drop_first_column['Mes y a??o'].notnull()]
df_drop_rows_nan


# In[10]:


from datetime import datetime
df_replaced_with_empty = df_drop_rows_nan.copy()
for i, item in enumerate(df_replaced_with_empty['Mes y a??o']):
    if isinstance(item, datetime):
        df_replaced_with_empty['Mes y a??o'][i] = item.strftime("%Y-%m")
        continue
    temp_list = item.split('-')
    year_value = '20'
    if len(temp_list) > 1:
        temp_year = temp_list[1]
        try: 
            int(temp_year)
            year_value = temp_year
#             df_replaced_with_empty['date'][i] = year_value + '-01'
        except ValueError:
            pass
    
    if temp_list:
        temp_month_val = temp_list[0]
        real_month_val = ''
        if temp_month_val == 'ene':
            real_month_val = '-01'
        elif temp_month_val == 'feb':
            real_month_val = '-02'
        elif temp_month_val == 'mar':
            real_month_val = '-03'
        elif temp_month_val == 'abr':
            real_month_val = '-04'
        elif temp_month_val == 'may':
            real_month_val = '-05'
        elif temp_month_val == 'jun':
            real_month_val = '-06'
        elif temp_month_val == 'jul':
            real_month_val = '-07'
        elif temp_month_val == 'JL':
            real_month_val = '-07'
        elif temp_month_val == 'ago':
            real_month_val = '-08'
        elif temp_month_val == 'sep':
            real_month_val = '-09'
        elif temp_month_val == 'oct':
            real_month_val = '-10'
        elif temp_month_val == 'nov':
            real_month_val = '-11'
        elif temp_month_val == 'dic':
            real_month_val = '-12'
            
        if int(year_value) > 70:
            df_replaced_with_empty['Mes y a??o'][i] = '19' + year_value + real_month_val + '-01'
        else:
            df_replaced_with_empty['Mes y a??o'][i] = '20' + year_value + real_month_val + '-01'
#         previous_month_val = temp_month_val
# df_replaced_with_empty['Mes y a??o'][4].split('-')[0] == 'may'
df_replaced_with_empty = df_replaced_with_empty.rename( columns={'Mes y a??o': 'Date'})
df_replaced_with_empty = df_replaced_with_empty.set_index('Date')
df_replaced_with_empty['country'] = 'Chile'
df_replaced_with_empty


# In[12]:


df_replaced_with_empty.index = pd.to_datetime(df_replaced_with_empty.index, format="%Y-%m-%d")

alphacast.datasets.dataset(127).upload_data_from_df(df_replaced_with_empty, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)





