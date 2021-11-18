#!/usr/bin/env python
# coding: utf-8

# ## About:
# El documento consta de dos partes:
# - Una sección de data historica, que debe correrse sólo la primera vez que éste documento se reproduce
# - Una sección de actualización

# In[11]:


import requests
import pandas as pd
from lxml import etree
from bs4 import BeautifulSoup
from datetime import date

from datetime import datetime
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[12]:


def remove_numbers(x):
    x = x.replace('1/', '')
    x = x.replace('2/', '')
    x = x.replace('3/', '')
    x = x.replace('4/', '')
    x = x.replace('*/', '')
    x = x.replace('á', 'a')
    x = x.replace('é', 'e')
    x = x.replace('í', 'i')
    x = x.replace('ó', 'o')
    x = x.replace('ú', 'u')
    x = x.replace('ADOUISICION', 'ADQUISICION')
    return x


# ## Descarga de data histórica

# In[13]:


#La info proviene de https://www.dipres.gob.cl/
#Para llegar hay que ir a Estadísticas de las Finanzas Públicas / Ejecución Presupuestaria / Operación Mensual
#Todos los archivos dicen Febrero u otro mes, por mas que sea diciembre

#Hay que corregir esto, para que no se cargue a mano cada año
historical_data = {
    '2020' :'articles-215720_doc_xls_informe_ej_febrero.xlsx',
    '2019': 'articles-198773_doc_xls_informe_ej_febrero.xlsx',
    '2018': 'articles-184750_doc_xls_informe_ej_febrero.xlsx',
    '2017': 'articles-169907_doc_xls_informe_ej_noviembre.xlsx',
    '2016': 'articles-156090_doc_xls_informe_ej_noviembre.xlsx',
    '2015': 'articles-142807_doc_xls_informe_ej_septiembre.xlsx',
    '2014': 'articles-128661_doc_xls_informe_ej_noviembre.xls',
    '2013': 'articles-112961_doc_xls_informe_ej_noviembre.xls',
     '2012': 'articles-96366_doc_xls_informe_ej_noviembre.xls',
     #'2011': 'articles-84195_doc_xls_informe_ej_noviembre.xls',
     #'2010': 'articles-70706_doc_xls_informe_ej_noviembre.xls',
     #'2009': 'articles-59560_doc_xls_informe_ej_diciembre.xls',
     #'2008': 'articles-42962_doc_xls_internetabrilval.xls',
     #'2007': 'articles-21338_doc_xls_informeejdiciembre.xls',
     #'2006': 'articles-24004_doc_xls_informeejdiciembre.xls'
}


# In[14]:


fiscal_chile = pd.DataFrame([])
base = ['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO', 'secciones', 'especific']
meses = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 
             'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 
             'Noviembre', 'Diciembre']
    
for year, file in historical_data.items():
    filename = 'https://www.dipres.gob.cl/598/{}'.format(file)
        
    temp_df = pd.DataFrame([])
    skiprows = 7
    name_sheets = ['Total', 'Pptario', 'Extrappt']
    
    for name in name_sheets:
        print(filename)
        temp = pd.read_excel(filename, skiprows = skiprows, sheet_name=name)
        column_names = list(temp.columns)
        column_names[0] = 'TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'
        column_names[1] = 'secciones'
        column_names[2] = 'especific'
        temp.columns = column_names
        maxrow = temp[temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO']=='FINANCIAMIENTO'].index[0]
        maxrows = int(maxrow+1)
        temp = temp[:maxrows]

        temp = temp.dropna(how='all', subset= temp.columns[2:])
        temp = temp.fillna('')
        temp = temp.astype({'TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO': str, 
                          'secciones': str,
                          'especific': str})

        columns_subset = []
        for column in temp.columns:
            if (column in base) or (column in meses):
                columns_subset += [column]

        temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] = temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'].apply(lambda x: remove_numbers(x))
        temp['secciones'] = temp['secciones'].apply(lambda x: remove_numbers(x))
        temp['especific'] = temp['especific'].apply(lambda x: remove_numbers(x))
        
        temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] = temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'].apply(lambda x: x.strip())
        temp['secciones'] = temp['secciones'].apply(lambda x: x.strip())
        temp['especific'] = temp['especific'].apply(lambda x: x.strip())
        
        def replace_startspace(x):
            x = str(x)
            if x[0:2] == '  ':
                x= x.replace('  ', '')
            return x
        
        temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] = temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'].apply(lambda x: replace_startspace(x))
        temp['secciones'] = temp['secciones'].apply(lambda x: replace_startspace(x))
        temp['especific'] = temp['especific'].apply(lambda x: replace_startspace(x))
        
        transac_name = ''
        lista_col = []
        for i, value in enumerate(temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO']):
            if value != '':
                transac_name = value
                lista_col += [value]
            elif value == '':
                lista_col += [transac_name]

        temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] = lista_col
        temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] = temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'].apply(lambda x: x.capitalize())
        temp = temp[columns_subset]

#         print(temp.columns)
        
        if name =='Pptario':
            temp = temp[(temp.iloc[:, 1] == 'Cobre bruto') | (temp.iloc[:, 1] == 'Intereses')]    
            temp['indexes'] = 'PRESUPUESTARIO -'+ temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] + ' ' + temp['secciones'] + ' ' + temp['especific']

        elif name == 'Extrappt':
            temp['indexes'] = 'EXTRAPRESUPUESTARIO -'+ temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] + ' ' + temp['secciones'] + ' ' + temp['especific']
            temp = temp[temp['secciones'] == 'Intereses']
        else:
             temp['indexes'] = 'TOTAL -'+ temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] + ' ' + temp['secciones'] + ' ' + temp['especific']
            
        def replace_double(x):
            x = x.replace('--','-')
            return x
        
        temp['indexes'] = temp['indexes'].apply(lambda x: replace_double(x))
        
        temp = temp[temp.columns[3:]]
        
        def replace_finalscore(x):
            x = str(x)
            if x[-1] == '-':
                x = x[:-1]
            return x
        temp['indexes'] = temp['indexes'].apply(lambda x: replace_finalscore(x))
        
        def replace_finalspace(x):
            x = str(x)
            if x[-2:] == '  ':
                x = x[:-2]
            elif x[-1] == ' ':
                x = x[:-1]
            return x
        temp['indexes'] = temp['indexes'].apply(lambda x: replace_finalspace(x))
        
        temp = temp.set_index('indexes')
        temp = temp.T
        def reindexar(x):
            if 'Enero' in x:
                x = year + x.replace('Enero', '-1-1') 
            elif 'Febrero' in x:
                x = year + x.replace('Febrero', '-2-1')
            elif 'Marzo' in x:
                x = year + x.replace('Marzo', '-3-1')
            elif 'Abril' in x:
                x = year + x.replace('Abril', '-4-1')
            elif 'Mayo' in x:
                x = year + x.replace('Mayo', '-5-1')
            elif 'Junio' in x:
                x = year + x.replace('Junio', '-6-1') 
            elif 'Julio' in x:
                x = year + x.replace('Julio', '-7-1')
            elif 'Agosto' in x:
                x = year + x.replace('Agosto', '-8-1')
            elif 'Septiembre' in x:
                x = year + x.replace('Septiembre', '-9-1')
            elif 'Octubre' in x:
                x = year + x.replace('Octubre', '-10-1')
            elif 'Noviembre' in x:
                x = year + x.replace('Noviembre', '-11-1')
            elif 'Diciembre' in x:
                x = year + x.replace('Diciembre', '-12-1')
            
            return x
        
        
        
        temp['indexes'] = temp.index
        temp['indexes'] = temp['indexes'].apply(lambda x: reindexar(x))
        temp = temp.rename(columns={'indexes':'Date'})
        temp['Date'] = temp['Date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
        temp = temp.set_index('Date')
 
        temp_df = pd.concat([temp_df, temp], axis=1)

    
    print(temp_df.shape)
    fiscal_chile = fiscal_chile.append(temp_df)


fiscal_chile['country'] = 'Chile'


# ## Parseo Data

# In[15]:


url = 'https://www.dipres.gob.cl/598/w3-propertyvalue-15491.html'
r = requests.get(url)
html = r.content
soup = BeautifulSoup(html, "html.parser")

years = {}
for link in soup.findAll('a'):
    if '#recuadros_articulo_' in link.get('href'):
        key = link.getText()
        value = link.get('href')
        years[key] = value

# print(years)
        
href_file = []
for link in soup.findAll('a'):
    if ('.xls' in link.get('href')) and ('Ejecución Presupuestaria' in link.getText()):
        temp = link.get('href')
        href_file += [temp]

href_file[0]


# In[16]:


parse_year=[]
for link in soup.findAll('a'):
    if link.get('href') == href_file[0]:
        text = link.getText()
        x = text.split(' ')
        parse_year += [x[-1]]


# In[17]:


parse_dict = {parse_year[0]: href_file[0]}
parse_dict


# ### Presupuestario: Coste bruto e Intereses

# In[18]:


fiscal_chile_update = pd.DataFrame([])
base = ['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO', 'secciones', 'especific']
meses = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 
             'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 
             'Noviembre', 'Diciembre']


for year, file in parse_dict.items():
    filename = 'https://www.dipres.gob.cl/598/{}'.format(file)
        
    temp_df = pd.DataFrame([])
    skiprows = 7
    name_sheets = ['Total', 'Pptario', 'Extrappt']
    
    for name in name_sheets:
        temp = pd.read_excel(filename, skiprows = skiprows, sheet_name=name)
        column_names = list(temp.columns)
        column_names[0] = 'TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'
        column_names[1] = 'secciones'
        column_names[2] = 'especific'
        temp.columns = column_names
        maxrow = temp[temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO']=='FINANCIAMIENTO'].index[0]
        maxrows = int(maxrow+1)
        temp = temp[:maxrows]
#         temp = temp.rename(columns={'Unnamed: 0':'TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO', 
#                                     'Unnamed: 1': 'secciones', 
#                                     'Unnamed: 2':'especific'})

        temp = temp.dropna(how='all', subset= temp.columns[2:])
        temp = temp.fillna('')
        temp = temp.astype({'TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO': str, 
                          'secciones': str,
                          'especific': str})

        columns_subset = []
        for column in temp.columns:
            if (column in base) or (column in meses):
                columns_subset += [column]

        temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] = temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'].apply(lambda x: remove_numbers(x))
        temp['secciones'] = temp['secciones'].apply(lambda x: remove_numbers(x))
        temp['especific'] = temp['especific'].apply(lambda x: remove_numbers(x))
        
        temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] = temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'].apply(lambda x: x.strip())
        temp['secciones'] = temp['secciones'].apply(lambda x: x.strip())
        temp['especific'] = temp['especific'].apply(lambda x: x.strip())
        
        def replace_startspace(x):
            x = str(x)
            if x[0:2] == '  ':
                x= x.replace('  ', '')
            return x
        
        temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] = temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'].apply(lambda x: replace_startspace(x))
        temp['secciones'] = temp['secciones'].apply(lambda x: replace_startspace(x))
        temp['especific'] = temp['especific'].apply(lambda x: replace_startspace(x))
        
        
        transac_name = ''
        lista_col = []
        for i, value in enumerate(temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO']):
            if value != '':
                transac_name = value
                lista_col += [value]
            elif value == '':
                lista_col += [transac_name]

        temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] = lista_col
        temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] = temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'].apply(lambda x: x.capitalize())
        temp = temp[columns_subset]

#         print(temp.columns)
        
        if name =='Pptario':
            temp = temp[(temp.iloc[:, 1] == 'Cobre bruto') | (temp.iloc[:, 1] == 'Intereses')]    
            temp['indexes'] = 'PRESUPUESTARIO -'+ temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] + ' ' + temp['secciones'] + ' ' + temp['especific']

        elif name == 'Extrappt':
            temp['indexes'] = 'EXTRAPRESUPUESTARIO -'+ temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] + ' ' + temp['secciones'] + ' ' + temp['especific']
            temp = temp[temp['secciones'] == 'Intereses']
        else:
             temp['indexes'] = 'TOTAL -'+ temp['TRANSACCIONES QUE AFECTAN EL PATRIMONIO NETO'] + ' ' + temp['secciones'] + ' ' + temp['especific']
            
        def replace_double(x):
            x = x.replace('--','-')
            return x
        
        temp['indexes'] = temp['indexes'].apply(lambda x: replace_double(x))
        
        temp = temp[temp.columns[3:]]
        
        def replace_finalscore(x):
            x = str(x)
            if x[-1] == '-':
                x = x[:-1]
            return x
        temp['indexes'] = temp['indexes'].apply(lambda x: replace_finalscore(x))

        
        def replace_finalspace(x):
            x = str(x)
            if (x[-1] == ' ') and (x[-2] == ' '):
                x = x[:-2]
            elif x[-1] == ' ':
                x = x[:-1]
            return x
        temp['indexes'] = temp['indexes'].apply(lambda x: replace_finalspace(x))
        
        temp = temp.set_index('indexes')
        temp = temp.T
        def reindexar(x):
            if 'Enero' in x:
                x = year + x.replace('Enero', '-1-1') 
            elif 'Febrero' in x:
                x = year + x.replace('Febrero', '-2-1')
            elif 'Marzo' in x:
                x = year + x.replace('Marzo', '-3-1')
            elif 'Abril' in x:
                x = year + x.replace('Abril', '-4-1')
            elif 'Mayo' in x:
                x = year + x.replace('Mayo', '-5-1')
            elif 'Junio' in x:
                x = year + x.replace('Junio', '-6-1') 
            elif 'Julio' in x:
                x = year + x.replace('Julio', '-7-1')
            elif 'Agosto' in x:
                x = year + x.replace('Agosto', '-8-1')
            elif 'Septiembre' in x:
                x = year + x.replace('Septiembre', '-9-1')
            elif 'Octubre' in x:
                x = year + x.replace('Octubre', '-10-1')
            elif 'Noviembre' in x:
                x = year + x.replace('Noviembre', '-11-1')
            elif 'Diciembre' in x:
                x = year + x.replace('Diciembre', '-12-1')
            
            return x
        
        
        
        temp['indexes'] = temp.index
        temp['indexes'] = temp['indexes'].apply(lambda x: reindexar(x))
        temp = temp.rename(columns={'indexes':'Date'})
        
        temp['Date'] = temp['Date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
        temp = temp.set_index('Date')
        
        
#         colstodrop = temp[(temp['indexes'][-5:] == 'Giros') | (temp['indexes'][-9:] == 'Depositos')]
#         temp.drop(colstodrop, inplace= True)
#         print(temp.shape)
 
        temp_df = pd.concat([temp_df, temp], axis=1)

    
    print(temp_df.shape)
    fiscal_chile_update = fiscal_chile.append(temp_df)

fiscal_chile_update['country'] = 'Chile'


# In[19]:


df = fiscal_chile.append(fiscal_chile_update).drop_duplicates(keep="first").sort_index()


alphacast.datasets.dataset(92).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




