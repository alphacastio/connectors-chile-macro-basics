#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
from datetime import datetime
import requests
import numpy as np
import re

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


loop = {
    1:{"sheet":'3'},
    2:{"sheet":'4'}, 
    3:{"sheet":'5'},
    4:{"sheet":'6'},
    5:{"sheet":'7'}, 
    6:{"sheet":'8'},
    7:{"sheet":'9'},
    8:{"sheet":'10'},   
}

dfFinal = pd.DataFrame()        
for key in loop.keys():

    df = pd.read_excel("https://www.ine.cl/docs/default-source/ventas-de-servicios/cuadro-estadisticos/base-promedio-a%C3%B1o-2014-100/series-mensuales-desde-enero-2014-a-la-fecha.xls?sfvrsn=2ba936a_54", skiprows=6, sheet_name=loop[key]["sheet"])

    df = df.loc[:, ~(df == '/R').any()]

    indiceFinal = df[df['Mes y año'] == '/R: Cifras rectificadas'].index[0]

    df1 = df.iloc[(indiceFinal+1):,0] 

    df = df.dropna(how='all').dropna(how='all',axis=1)

    df =df.dropna(how='all',axis=1)

    df = df.dropna(how='all', subset=df.columns[1:])

    def dateWrangler(x):
        global y
        x=str(x)
        list_x = x.split('-')
        if list_x[0] == 'ene':
            y = '20' + list_x[1]+'-01-01'
        elif list_x[0] == 'feb':
            y = '20' + list_x[1]+'-02-01'
        elif list_x[0] == 'mar':
            y = '20' + list_x[1]+'-03-01'
        elif list_x[0] == 'abr':
            y = '20' + list_x[1]+'-04-01'
        elif list_x[0] == 'may':
            y = '20' + list_x[1]+'-05-01'
        elif list_x[0] == 'jun':
            y = '20' + list_x[1]+'-06-01'
        elif list_x[0] == 'jul':
            y = '20' + list_x[1]+'-07-01'
        elif list_x[0] == 'ago':
            y = '20' + list_x[1]+'-08-01'
        elif list_x[0] == 'sep':
            y = '20' + list_x[1]+'-09-01'    
        elif list_x[0] == 'oct':
            y = '20' + list_x[1]+'-10-01'
        elif list_x[0] == 'nov':
            y = '20' + list_x[1]+'-11-01'
        elif list_x[0] == 'dic':
            y = '20' + list_x[1]+'-12-01'    
        return y

    df['Mes y año'] = df['Mes y año'].apply(lambda x: dateWrangler(x))

    df = df.rename({'Mes y año': 'Date'}, axis=1)        

    df['Date']=pd.to_datetime(df['Date'])

    df = df.set_index('Date')
    
    dfFinal = dfFinal.merge(df, how='outer', left_index=True, right_index=True)
    
    dfFinal = dfFinal.rename({4921: 'Transporte urbano y suburbano de pasajeros por vía terrestre (Metro de Santiago y RED)', 49225: 'Transporte de pasajeros en buses interurbanos', 4923: 'Transporte de carga por carretera', 50: 'Transporte por vía acuática' , 
                              51: 'Transporte por vía aérea', 521: 'Almacenamiento y depósito', 52213: 'Servicios prestados por concesionarios de carreteras', 5222: 'Actividades de servicios vinculadas al transporte acuático (puertos)', 
                              5229: 'Otras actividades de apoyo al transporte', 53: 'Actividades postales y de mensajería', 551: 'Actividades de alojamiento para estancias cortas', 561: 'Actividades de restaurantes y de servicio móvil de comidas',
                              562: 'Suministro de comidas por encargo y otras actividades de servicio de comidas' , 58: 'Actividades de edición', 60: 'Actividades de programación y transmisión',
                              61: 'Telecomunicaciones', 62: 'Programación informática, consultoría de informática y actividades conexas', 63: 'Actividades de servicios de información', 681: 'Actividades inmobiliarias realizadas con bienes propios o arrendados',
                              682: 'Actividades inmobiliarias realizadas a cambio de una retribución o por contrata', 691: 'Actividades jurídicas', 692: 'Actividades de contabilidad, teneduría de libros y auditoría; consultoría fiscal',
                              70: 'Actividades de oficinas principales; actividades de consultoría de gestión', 711: 'Actividades de arquitectura e ingeniería y actividades conexas de consultoría técnica', 731: 'Publicidad', 
                              771: 'Alquiler y arrendamiento de vehículos automotores', 773: 'Alquiler y arrendamiento de otros tipos de maquinaria, equipo y bienes tangibles', 78: 'Actividades de empleo', 80: 'Actividades de seguridad e investigación',
                              81: 'Actividades de servicios a edificios y de paisajismo', 822: 'Actividades de call-center', 829: 'Actividades de servicios de apoyo a las empresas', 90: 'Actividades creativas, artísticas y de entretenimiento',
                              92: 'Actividades de juegos de azar y apuestas', 93: 'Actividades deportivas, de esparcimiento y recreativas', 96: 'Otras actividades de servicios personales'
                              }, axis=1)  
    
dfFinal['country'] = 'Chile'  

alphacast.datasets.dataset(314).upload_data_from_df(dfFinal, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
