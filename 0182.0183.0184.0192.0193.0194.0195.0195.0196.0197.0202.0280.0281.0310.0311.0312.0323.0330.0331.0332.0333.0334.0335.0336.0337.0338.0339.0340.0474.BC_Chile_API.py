#!/usr/bin/env python
# coding: utf-8

# In[19]:


#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd

import requests
import numpy as np
from datetime import date
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

# In[20]:


def fix_with_index():
    df_indice = pd.read_html(url.format("INDEX"), attrs = {'id': "grilla"}, thousands='.', decimal=",")[0].set_index("Serie").transpose()

    for column in df_indice.columns:
        df_indice[column] = df_indice[column] / df_indice[column][-1]
        df_indice = df_indice.rename(columns={column: column + " - INDEX"})

    columnas_originales = df_orig.columns
    df_orig = df_orig.merge(df_indice, how="left", left_index=True, right_index=True)    

    for column in columnas_originales:
        if not(np.isnan(df_orig[column + " - INDEX"][-1])):
            df_orig[column] = df_orig[column][-1] * df_orig[column + " - INDEX"]
        del df_orig[column + " - INDEX"]
    df_indice = None


# In[24]:


def fix_date(df, frequency):
    if frequency == "QUARTERLY":
        df = df_merge
        df["Date"] = df.index
        df["Day"] = 1
        df["Month"] = df["Date"].str.split(pat=".", expand=True)[0]
        df["Year"] = df["Date"].str.split(pat=".", expand=True)[1]
        df["Month"] = df["Month"].replace('IV', 10)
        df["Month"] = df["Month"].replace('III', 7)            
        df["Month"] = df["Month"].replace('II', 4)
        df["Month"] = df["Month"].replace('I', 1)
            
    if frequency == "DAILY":
        df = df_merge
        df["Date"] = df.index
        df["Day"] = df["Date"].str.split(pat=".", expand=True)[0]
        df["Month"] = df["Date"].str.split(pat=".", expand=True)[1]
        df["Year"] = df["Date"].str.split(pat=".", expand=True)[2]
        df["Month"] = df["Month"].replace(
            {'Abr': 4, 'Ago': 8, 
             'Dic':12, 'Ene': 1, 
             'Feb': 2,'Jul': 7, 
             'Jun': 6, 'Mar': 3, 
             'May': 5 , 'Nov': 11, 
             'Oct': 10, 'Sep':9 }
            )
        
    if frequency == "MONTHLY":
        df = df_merge
        df["Date"] = df.index
        df["Day"] = 1
        df["Month"] = df["Date"].str.split(pat=".", expand=True)[0]
        df["Year"] = df["Date"].str.split(pat=".", expand=True)[1]
        df["Month"] = df["Month"].replace(
            {'Abr': 4, 'Ago': 8, 
             'Dic':12, 'Ene': 1, 
             'Feb': 2,'Jul': 7, 
             'Jun': 6, 'Mar': 3, 
             'May': 5 , 'Nov': 11, 
             'Oct': 10, 'Sep':9 }
            )        
        
    if frequency == "ANNUAL":
        df = df_merge

        df["Day"] = 1
        df["Month"] = 2
        df["Year"] = df.index
        
    df["Date"] = pd.to_datetime(df[["Year", "Month", "Day"]], errors="coerce")
    del df["Year"]
    del df["Month"]
    del df["Day"]
    df = df[df["Date"].notnull()]
    df = df.set_index("Date")
    return df


# In[21]:


import numpy as np

def expand_levels(df, sublevels):
    df = df.transpose().reset_index()
    df["level_1"] = df["index"]

    for sublevel in sublevels:
        df[sublevel] = np.nan
        print(sublevel)
        df.loc[df["level_1"].isin(sublevels[sublevel]), sublevel] = df["level_1"]
        df.loc[df["level_1"].isin(sublevels[sublevel]), "level_1"] = np.nan


    for x in range(len(sublevels), 0,  -1):
        print(x)
        df["level_{}_temp".format(x)] = df["level_{}".format(x)].ffill()
        df.loc[df["level_{}".format(x+1)].notnull(), "level_{}".format(x)] = df["level_{}_temp".format(x)]

    df["concept"] = df["level_1"]
    for x in range(2, len(sublevels)+2):
        df.loc[ df["level_{}".format(x)].notnull() , "concept"] = df["concept"] + " - " + df["level_{}".format(x)]

    df = df[df.columns.drop(list(df.filter(regex='level_')))]
    #df["Date"] = df["concept"]
    del df["index"]
    df = df.set_index("concept")
    df = df.transpose()
    return df


# In[24]:


# In[31]:


today_year = date.today().year

url = "https://si3.bcentral.cl/Siete/ES/Siete/Cuadro/"
url_params_monthly = "?cbFechaInicio={}&cbFechaTermino="+ str(today_year) + "&cbFrecuencia={}&cbCalculo={}&cbFechaBase="
url_params_daily = "?cbFechaDiaria={}&cbFrecuencia={}&cbCalculo={}&cbFechaBase="


#cbFrecuencia es la frequencya del dataset: DAILY, MONTHLY, QUARTERLY o ANNUAL
#fix_with_index es para arreglar el problema de cuando no trae decimales. Poner true si no trae decimales
#cbFechaInicio Pone la fecha inicial desde la que trae la data
#url_prefix le pone prefix a las distinas urls que componen el dataset
#sublevels separa los niveles si la data tiene mas de uno

param =  { 182: {
                "cbFrecuencia": "DAILY", 
                "fix_with_index": True,
                "cbFechaInicio": 2011,
                "url_prefix": [ ["CAP_DYB/MN_ESTAD_MON55/EM_BMAM_DM1/E1111", "Monetary Base"],
                               ["CAP_DYB/MN_ESTAD_MON55/EM_BMAM_DM2/E1112", "M1"],
                               ["CAP_DYB/MN_ESTAD_MON55/EM_BMAM_DM3/E1113", "M2"],
                               ["CAP_DYB/MN_ESTAD_MON55/EM_BMAM_DM4/E1114", "M3"]]
                }, 

          
             183: {
                            "cbFrecuencia": "DAILY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 1997,
                            "url_prefix": [ 
                                            ["CAP_TASA_INTERES/MN_TASA_INTERES_09/TPM_C1/T12", "Monetary policy reference rates"],
                                            ["CAP_TASA_INTERES/MN_TASA_INTERES_09/TMS_15/T311", "Secondary Market Rates - in $"],
                                            ["CAP_TASA_INTERES/MN_TASA_INTERES_09/TMS_16/T312", "Secondary Market Rates - in UF"],
                                            ["CAP_TASA_INTERES/MN_TASA_INTERES_09/EM_OMA_30", "Financial System - Interbank market"],
                                            ["CAP_TASA_INTERES/MN_TASA_INTERES_09/TSF_TCOLD/T41", "Financial System - Placements"],
                                            ["CAP_TASA_INTERES/MN_TASA_INTERES_09/TSF_TCAPD/T42", "Financial System - Deposits"],

                                         ]
                            }, 
           184: {
                            "cbFrecuencia": "MONTHLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 1997,
                            "url_prefix": [ 
                                            ["CAP_IND_SEC/MN_IND_SEC20/IS_P_SOF_INE/IS_P_SOF_INE", "Industrial Production"],
                                            ["CAP_IND_SEC/MN_IND_SEC20/IS_V_SOF_INE/IS_V_SOF_INE", "Industrial Sales"],
                                         ]
                            }, 
           192: {
                            "cbFrecuencia": "QUARTERLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 1997,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP6M_RES01", "BoP"]
                                         ]
                            },
           193: {
                            "cbFrecuencia": "QUARTERLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 1997,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP6M_PII_CF/BP6M_PII_CF", "PII"]
                                         ]
                            },
           194: {
                            "cbFrecuencia": "MONTHLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 1997,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP_RESERVAS_T/BP_RESERVAS_T", "Reservas internacionales"]
                                         ],
                           "drop_columns": ["DEG (Moneda: DEG)"]
                            },
          195: {
                            "cbFrecuencia": "MONTHLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 1997,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP6M_DEB_VM/BP6M_DEB_VM", "Deuda externa"]
                                         ]
                            },
          196: {
                            "cbFrecuencia": "MONTHLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 1997,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP6M_EXPORT/BP6M_EXPORT", ""]
                                         ],
                            "sublevels": { 
                                "level_2": ["Alimentos", "Bebidas y tabaco", "Carbonato de litio", "Celulosa, papel y otros", "Cobre", "Concentrado de molibdeno", "Forestal y muebles de madera", "Hierro", "Industria metálica básica", "Oro", "Otros agropecuarios", "Otros productos industriales", "Pesca extractiva", "Plata", "Productos metálicos, maquinaria y equipos", "Productos químicos", "Sal marina y de mesa", "Sector frutícola", "Sector silvícola", ],
                                "level_3": ["Fruta congelada", "Abonos", "Aceite de pescado", "Alambre de cobre", "Arándanos", "Bebidas no alcohólicas", "Carne de ave", "Carne de cerdo", "Cartulina", "Cátodos", "Celulosa blanqueada y semiblanqueada de conífera", "Celulosa blanqueada y semiblanqueada de eucaliptus", "Celulosa cruda de conífera", "Cereza", "Chips de madera", "Ciruela", "Concentrados", "Conservas de pescado", "Ferromolibdeno", "Fruta deshidratada", "Fruta en conserva", "Harina de pescado", "Jugo de fruta", "Kiwi", "Madera aserrada", "Madera contrachapada", "Madera perfilada", "Manufacturas metálicas", "Manzana", "Maquinaria y equipos", "Material de transporte", "Merluza", "Metanol", "Moluscos y crustáceos", "Neumáticos", "Nitrato de potasio", "Oxido de molibdeno", "Palta", "Pera", "Salmón", "Semilla de hortalizas", "Semilla de maíz", "Tableros de fibra de madera", "Trucha", "Uva", "Vino a granel y otros", "Vino embotellado", "Yodo", ]
                                    }
                            
              
              
                            },
          197: {
                            "cbFrecuencia": "MONTHLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 1997,
                            "url_prefix": [ 
                                            ["CAP_TIPO_CAMBIO/MN_TIPO_CAMBIO4/TCB_531_IND_TCRYCOMP/TCB_531", "Tipos de cambio"]
                                         ]
                            },
          202: {
                            "cbFrecuencia": "MONTHLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 1997,
                            "url_prefix": [ 
                                            ["CAP_DYB/MN_ESTAD_MON55/EM_CDEU_BI/E21", "Colocaciones"]
                                         ]
                            },
          280: {
                            "cbFrecuencia": "QUARTERLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 1996,
                            "url_prefix": [ 
                                            ["CAP_CCNN/MN_CCNN76/CCNN2013_G2/CCNN2013_G2", ""]
                                         ],
                            "sublevels": { 
                                "level_2": ["Construcción y otras obras", "Consumo de hogares e IPSFL", "Consumo Gobierno", "Exportación Bienes", "Exportación servicios", "Importación bienes", "Importación servicios", "Maquinaria y equipo", ],
                                "level_3": ["Agropecuario-silvícola -pesca", "Agropecuario-silvícola-pesca", "Bienes durables", "Bienes no durables", "Industria", "Minería", "Servicios", ],
                                "level_4": ["Cobre", "Resto", ]
                                    }
              
                            } ,
          281: {
                            "cbFrecuencia": "QUARTERLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 1996,
                            "url_prefix": [ 
                                            ["CAP_CCNN/MN_CCNN76/CCNN2013_P2/CCNN2013_P2", ""]
                                         ],
                            "sublevels": { 
                                "level_2": ["Alimentos, bebidas y tabaco", "Celulosa, papel e imprentas", "Comercio", "Maderas y muebles", "Minerales no metálicos y metálica básica", "Minería del cobre", "Otras actividades mineras", "Productos metálicos, maquinaria, equipos y otros", "Química, petróleo, caucho y plástico", "Restaurantes y hoteles", "Servicios empresariales", "Servicios financieros", "Textil, prendas de vestir, cuero y calzado", ],
                                "level_3": ["Alimentos", "Bebidas y tabaco", "Química, caucho y plástico", "Refinación de petróleo", ],
                                    }
              
                            },
          310: {
                            "cbFrecuencia": "ANNUAL", 
                            "fix_with_index": False,
                            "cbFechaInicio": 1996,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP6M_DE_PUB_PRV/BP6M_DE_PUB_PRV", ""]
                                         ],
                            "sublevels": { 
                                "level_2": ["Sector privado","Sector público" ],
                                "level_3": ["Banco Central", "Bancos", "Empresas no financieras", "Gobierno General", "Otras sociedades financieras", ]
                                    }
              
                            },        

          311: {
                            "cbFrecuencia": "MONTHLY", 
                            "fix_with_index": True,
                            "cbFechaInicio": 1996,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP6M_CCMM/BP6M_CCMM", ""]
                                         ],
                            "sublevels": { 
                                "level_2": ["Bienes de capital", "Bienes de consumo", "Bienes intermedios", ],
                                "level_3": ["Aparatos electrónicos de comunicación", "Aparatos médicos", "Bombas y compresores", "Buses", "Calderas de vapor", "Camiones y vehículos de carga", "Durables", "Equipos computacionales", "Maquinaria para la minería y la construcción", "Motores y turbinas", "Motores, generadores y transformadores eléctricos", "Otra maquinaria", "Otros bienes de consumo", "Otros vehículos de transporte", "Productos energéticos", "Resto bienes intermedios", "Semidurables", ],
                                "level_4": ["Abono", "Aceite lubricante", "Aparatos de control eléctrico", "Automóviles", "Azúcar y endulzante", "Bebidas y alcoholes", "Calzado", "Carbón mineral", "Carne", "Cartón y papel elaborados, y otros", "Celulares", "Computadores", "Concentrado de molibdeno", "Diésel", "Electrodomésticos", "Fibra y tejido", "Gas licuado", "Gas natural gaseoso", "Gas natural licuado", "Gasolinas", "Medicamentos", "Otros alimentos", "Partes y piezas de maquinaria para la minería y la construcción", "Partes y piezas de otras maquinarias y equipos", "Perfumes", "Petróleo", "Productos metálicos", "Productos químicos", "Televisores", "Trigo y maíz", "Vestuario", ]
                                
                                    }
              
                            }                ,

          312: {
                            "cbFrecuencia": "MONTHLY", 
                            "fix_with_index": True,
                            "cbFechaInicio": 1996,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP6M_EXP_CUCI/BP6M_EXP_CUCI", ""]
                                         ],
                            "sublevels": { 
                                "level_2": ["Abonos (excepto los del grupo 272)", "Artículos manufacturados", "Bebidas", "Carne y preparados de carne", "Cereales y preparados de cereales", "Concentrados y desechos de metales", "Corcho y madera", "Forraje para animales (excepto cereales sin moler)", "Hilados, tejidos, artículos confeccionados de fibras textiles, N.E.P., y productos conexos", "Legumbres y frutas", "Manufacturas de corcho y de madera (excepto muebles)", "Manufacturas de metales, N.E.P.", "Metales no ferrosos", "Otros artículos manufacturados", "Otros artículos manufacturados diversos, N.E.P", "Otros bebidas y tabaco", "Otros combustibles y lubricantes, minerales y productos conexos", "Otros maquinaria y equipo de transporte", "Otros materiales crudos no comestibles", "Otros productos alimenticios y animales vivos", "Otros productos químicos y productos conexos, N.E.P", "Papel, cartón y artículos de pasta de papel, de papel o de cartón", "Pasta y desperdicio de papel", "Pescado (excepto mamíferos marinos), crustáceos, moluscos e invertebrados acuáticos y sus preparados", "Petróleo, productos derivados del petróleo y productos conexos", "Productos animales y vegetales", "Productos químicos inorgánicos", "Productos químicos orgánicos", "Productos y preparados comestibles diversos", "Vehículos de carretera (incluso aerodeslizadores)", ],
                                
                                    }
              
                            }                ,          

            323: {
                    "cbFrecuencia": "MONTHLY", 
                    "fix_with_index": False,
                    "cbFechaInicio": 2000,
                    "url_prefix": [ 
                                    ["CAP_IND_SEC/MN_IND_SEC20/IS_P_SOF_INE/IS_P_SOF_INE", "Industrial Production"],
                                 ],

                    "keep_cols": ["Industrial Production - Producción industrial INE (base 2014=100)",
                                  "Industrial Production - Producción industrial INE (base 2014=100) desestacionalizada",
                                  "Industrial Production - Producción industrial INE (base 2009=100)"]

            },
          
          330: {
                            "cbFrecuencia": "QUARTERLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 2008,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP6M_IND_EXPORT_CR2013_B/BP6M_IND_EXPORT_CR2013_B", ""]
                                         ],
                            "sublevels": { 
                                "level_2": ["Alimentos", "Bebidas y tabaco", "Celulosa, papel y otros", "Cobre", "Combustibles, caucho y plástico", "Frutas", "Madera y Muebles", "Otros minerales", "Otros productos industriales", "Productos metálicos básicos", "Productos metálicos y maquinaria y equipos", "Productos químicos", "Resto agropecuarios, silvícola y pesca", ],
                                "level_3": ["Cátodos", "Celulosa", "Cobre sin refinar", "Concentrados", "Conservas de frutas y vegetales", "Duraznos, cerezas y otros carozos", "Hierro", "Madera aserrada, cepillada y astillada", "Oro", "Óxidos de molibdeno", "Papel e impresos", "Resto", "Resto alimentos", "Resto bebidas y tabaco", "Resto frutas", "Resto madera y muebles", "Resto químicos", "Salmón y trucha refrigerados o congelados", "Tableros y madera prensada", "Uvas", "Vinos", ],
                                
                                    }
              
                            }                ,          
          
          331: {
                            "cbFrecuencia": "QUARTERLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 2008,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP6M_CCCXT_CR2013_B/BP6M_CCCXT_CR2013_B", ""]
                                         ],
                            "sublevels": { 
                                "level_2": ["Alimentos", "Bebidas y tabaco", "Celulosa, papel y otros", "Cobre", "Combustibles, caucho y plástico", "Frutas", "Madera y Muebles", "Otros minerales", "Otros productos industriales", "Productos metálicos básicos", "Productos metálicos y maquinaria y equipos", "Productos químicos", "Resto agropecuarios, silvícola y pesca", ],
                                "level_3": ["Cátodos", "Celulosa", "Cobre sin refinar", "Concentrados", "Conservas de frutas y vegetales", "Duraznos, cerezas y otros carozos", "Hierro", "Madera aserrada, cepillada y astillada", "Oro", "Óxidos de molibdeno", "Papel e impresos", "Resto", "Resto alimentos", "Resto bebidas y tabaco", "Resto frutas", "Resto madera y muebles", "Resto químicos", "Salmón y trucha refrigerados o congelados", "Tableros y madera prensada", "Uvas", "Vinos", ],
                                
                                    }
              
                            }                ,               
          
          332: {
                            "cbFrecuencia": "QUARTERLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 2008,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP6M_INV_EXPORT_CR2013_B/BP6M_INV_EXPORT_CR2013_B", ""]
                                         ],
                            "sublevels": { 
                                "level_2": ["Alimentos", "Bebidas y tabaco", "Celulosa, papel y otros", "Cobre", "Combustibles, caucho y plástico", "Frutas", "Madera y Muebles", "Otros minerales", "Otros productos industriales", "Productos metálicos básicos", "Productos metálicos y maquinaria y equipos", "Productos químicos", "Resto agropecuarios, silvícola y pesca", ],
                                "level_3": ["Cátodos", "Celulosa", "Cobre sin refinar", "Concentrados", "Conservas de frutas y vegetales", "Duraznos, cerezas y otros carozos", "Hierro", "Madera aserrada, cepillada y astillada", "Oro", "Óxidos de molibdeno", "Papel e impresos", "Resto", "Resto alimentos", "Resto bebidas y tabaco", "Resto frutas", "Resto madera y muebles", "Resto químicos", "Salmón y trucha refrigerados o congelados", "Tableros y madera prensada", "Uvas", "Vinos", ],
                                
                                    }
              
                            }                ,               

          333: {
                            "cbFrecuencia": "QUARTERLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 2008,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP6M_IND_IMPORT_CR2013_B/BP6M_IND_IMPORT_CR2013_B", ""]
                                         ],
                            "sublevels": { 
                                "level_2": ["Bienes Durables", "Bienes No durables", "Productos energéticos", "Resto bienes intermedios", ],
                                "level_3": ["Diésel", "Petróleo", "Resto productos energéticos"],
                                
                                    }
              
                            }                ,              
          334: {
                            "cbFrecuencia": "QUARTERLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 2008,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP6M_CCCMT_CR2013_B/BP6M_CCCMT_CR2013_B", ""]
                                         ],
                            "sublevels": { 
                                "level_2": ["Bienes Durables", "Bienes No durables", "Productos energéticos", "Resto bienes intermedios", ],
                                "level_3": ["Diésel", "Petróleo", "Resto productos energéticos"],
                                
                                    }
              
                            }                , 
          335: {
                            "cbFrecuencia": "QUARTERLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 2008,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP6M_INV_IMPORT_CR2013_B/BP6M_INV_IMPORT_CR2013_B", ""]
                                         ],
                            "sublevels": { 
                                "level_2": ["Bienes Durables", "Bienes No durables", "Productos energéticos", "Resto bienes intermedios", ],
                                "level_3": ["Diésel", "Petróleo", "Resto productos energéticos"],
                                
                                    }
              
                            }                ,           

          336: {
                            "cbFrecuencia": "QUARTERLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 2003,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP6M_EXPORT_SERV/BP6M_EXPORT_SERVC", "Exports"],
                                            ["CAP_BDP/MN_BDP42/BP6M_IMPORT_SERV/BP6M_IMPORT_SERVC", "Imports"]
                                         ],
                            "sublevels": { 
                                "level_2": ["Transporte marítimo", "Otros transportes"],
                                
                                    }
              
                            }                ,                  
          337: {
                            "cbFrecuencia": "DAILY", 
                            "fix_with_index": True,
                            "cbFechaInicio": 1982,
                            "url_prefix": [ 
                                            ["CAP_TIPO_CAMBIO/MN_TIPO_CAMBIO4/DOLAR_OBS_ADO/TCB_505", "Chilean Peso"],
                                            ["CAP_TIPO_CAMBIO/MN_TIPO_CAMBIO4/TCB_510_PARIDADES/TCB_510", ""]
                                         ],
                            }                ,                

          338: {
                            "cbFrecuencia": "MONTHLY", 
                            "fix_with_index": True,
                            "cbFechaInicio": 1970,
                            "url_prefix": [ 
                                            ["CAP_TIPO_CAMBIO/MN_TIPO_CAMBIO4/TC_HIST/TC_HIST", ""],
                                         ],
                            }                ,    
          339: {
                            "cbFrecuencia": "MONTHLY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 1984,
                            "url_prefix": [ 
                                            ["CAP_TIPO_CAMBIO/MN_TIPO_CAMBIO4/TCB_531_IND_TCRYCOMP/TCB_531", ""],
                                         ],
                            }                ,         
          340: {
                            "cbFrecuencia": "DAILY", 
                            "fix_with_index": False,
                            "cbFechaInicio": 1995,
                            "url_prefix": [ 
                                            ["CAP_TIPO_CAMBIO/MN_TIPO_CAMBIO4/TCB_532_IND_TCM/TCB_532", ""],
                                         ],
                            }                ,    
          474: {
                            "cbFrecuencia": "MONTHLY", 
                            "fix_with_index": True,
                            "cbFechaInicio": 1996,
                            "url_prefix": [ 
                                            ["CAP_BDP/MN_BDP42/BP6M_CCMM/BP6M_CCMM", ""],
                                            ["CAP_BDP/MN_BDP42/BP6M_EXPORT/BP6M_EXPORT", ""],
                                            ["CAP_BDP/MN_BDP42/BP6M_BAL_COM/BP6M_BAL_COM", ""]
                                         ], 
                            "keep_cols": ["Total importaciones de bienes (CIF)", 
                                         "Exportaciones",
                                         "Saldo de balanza comercial"]
            },

    }


# In[23]:


for key in param.keys():
#for key in [323]:
    df_merge = pd.DataFrame()
    df_orig = None
    database = param[key]
    for url_prefix in database["url_prefix"]:
        if database["cbFrecuencia"] == "DAILY":
            df_append = pd.DataFrame()
            for year in range(database["cbFechaInicio"],today_year + 1):
                full_url = url + url_prefix[0] + url_params_daily.format(year, database["cbFrecuencia"], "NONE")
                print(full_url)                
                df_append = df_append.append(pd.read_html(full_url, attrs = {'id': "grilla"}, thousands='.', decimal=",")[0].set_index("Serie").transpose())                
                print(df_append.shape)
            df_orig = df_append
            df_append = None
        else:
                full_url = url + url_prefix[0] + url_params_monthly.format(database["cbFechaInicio"], database["cbFrecuencia"], "NONE")
                print(full_url)
                df_orig = pd.read_html(full_url, attrs = {'id': "grilla"}, thousands='.', decimal=",")[0].set_index("Serie").transpose()            
                df_orig = df_orig[df_orig.index!="Sel."]

        if "drop_columns" in database.keys():
            for col in database["drop_columns"]:
                del df_orig[col]
                
        for column in df_orig.columns:
            if url_prefix[1] != "":
                df_orig = df_orig.rename(columns={column: url_prefix[1] + " - " + column})
                df_orig = df_orig[df_orig.index!="Sel."]

        print(df_orig.shape)
        df_merge = df_merge.merge(df_orig, how="outer", left_index=True, right_index=True)       
        print(df_merge.shape)
        
    df = df_merge
    df = fix_date(df, database["cbFrecuencia"])
    df = df.loc[:,~df.columns.duplicated()]
#     if database["cbFrecuencia"] == "QUARTERLY":
    if "sublevels" in database.keys():
        df = expand_levels(df, database["sublevels"])
        df.index = pd.to_datetime(df.index)
    df = df.transpose().drop_duplicates().transpose()
    if "keep_cols" in database.keys():
        df = df[database["keep_cols"]]
    df["country"] = "Chile"
    
    alphacast.datasets.dataset(key).upload_data_from_df(df, 
        deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

# In[ ]:




