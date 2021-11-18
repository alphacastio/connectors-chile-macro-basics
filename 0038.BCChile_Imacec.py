#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[2]:


def def_and_fix_date(df):
    df["year"] = df["Serie"].str.split(".", expand=True, n=1)[1]
    df["month"] = df["Serie"].str.split(".", expand=True, n=1)[0]
    df["month"] = df["month"].replace('Ene', 1 ) 
    df["month"] = df["month"].replace('Feb',2) 
    df["month"] = df["month"].replace('Mar',3) 
    df["month"] = df["month"].replace('Abr',4) 
    df["month"] = df["month"].replace('May',5) 
    df["month"] = df["month"].replace('Jun',6) 
    df["month"] = df["month"].replace('Jul',7) 
    df["month"] = df["month"].replace('Ago',8) 
    df["month"] = df["month"].replace('Sep',9)
    df["month"] = df["month"].replace('Oct',10)
    df["month"] = df["month"].replace('Nov',11) 
    df["month"] = df["month"].replace('Dic',12)
    df["day"] = 1
    df["Date"] = pd.to_datetime(df[["year", "month", "day"]])
    del df["year"]
    del df["month"]
    del df["day"]
    del df["Serie"]
    df = df.set_index("Date")
    return df


# In[3]:


header = {
"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
"Accept-Encoding": "gzip, deflate, br",
"Accept-Language": "es-ES,es;q=0.9",
"Cache-Control": "no-cache",
"Connection": "keep-alive",
"Host": "si3.bcentral.cl",
"Pragma": "no-cache",
"Referer": "https://si3.bcentral.cl/",
"Sec-Fetch-Dest": "document",
"Sec-Fetch-Mode": "navigate",
"Sec-Fetch-Site": "same-origin",
"Sec-Fetch-User": "?1",
"Upgrade-Insecure-Requests": "1",
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36"
}

form_data =     {
    "cbFechaInicio": "1996",
    "cbFrecuencia": "MONTHLY",
    "cbCalculo": "NONE"
    }


# In[4]:



ids = ["CAP_CCNN","MN_CCNN76","CCNN2013_IMACEC_01"]
ids2 = ["CAP_CCNN","MN_CCNN76","CCNN2013_IMACEC_03"]

string_ids = "/".join(ids)
string_ids2 = "/".join(ids2)

URL = "https://si3.bcentral.cl/Siete/ES/Siete/Cuadro/{}?cbFechaInicio=2001&cbFrecuencia=MONTHLY&cbCalculo=NONE".format(string_ids)

f = requests.post(URL, data=form_data, headers=header)
df = pd.read_html(f.text, attrs={"id": "grilla"}, thousands="")[0]

URL2 = "https://si3.bcentral.cl/Siete/ES/Siete/Cuadro/{}?cbFechaInicio=2001&cbFrecuencia=MONTHLY&cbCalculo=NONE".format(string_ids2)

f2 = requests.post(URL2, data=form_data, headers=header)
df2 = pd.read_html(f2.text, attrs={"id": "grilla"}, thousands="")[0]

df2["Serie"] = df2["Serie"] + " - sa_orig"



# In[5]:


del df["Sel."]
df = df.transpose().reset_index()
new_header = df.iloc[0] #grab the first row for the header
df = df[1:] #take the data less the header row
df.columns = new_header #set the header row as the df header
df = def_and_fix_date(df)
for column in df.columns:
    df[column] = df[column].str.replace(",", ".").astype("float")

del df2["Sel."]
df2 = df2.transpose().reset_index()
new_header2 = df2.iloc[0] #grab the first row for the header
df2 = df2[1:] #take the data less the header row
df2.columns = new_header2 #set the header row as the df header
df2 = def_and_fix_date(df2)
for column in df2.columns:
    df2[column] = df2[column].str.replace(",", ".").astype("float")


# In[6]:


df3 = pd.merge(df,df2, right_index=True, left_index=True)


# In[7]:


df3["country"] = "Chile"

alphacast.datasets.dataset(38).upload_data_from_df(df3, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
