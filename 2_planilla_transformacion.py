import pandas as pd
import numpy as np
from datetime import datetime
pd.set_option('display.max_columns', 30)
from tqdm import tqdm
import os
from os import listdir
from os.path import isfile, join
import re
import string
import matplotlib.pyplot as plt
import time
from collections import defaultdict
# ====================================================================================================================================
def obtain_date(fecha):
    if fecha != None and isinstance(fecha, str):
        try:
            dt_1 = datetime.strptime(fecha, '%H:%M')
        except:
            dt_1 = datetime.strptime(fecha, '%m/%d/%Y %I:%M:%S %p')
    else:
        dt_1 = datetime.now()
    return dt_1

def obtener_files():
    directorio = os.getcwd()
    directorio_files = directorio + '/planilla_mensual'
    onlyfiles = [f for f in listdir(directorio_files) if isfile(join(directorio_files, f))]
    return onlyfiles

def obtener_df_de_firestore():
    onlyfiles = obtener_files()
    df_firestore = pd.DataFrame()
    for i in tqdm(onlyfiles):
        df_firestore = pd.concat([df_firestore, pd.read_csv(os.getcwd() + '/planilla_mensual/' + i, low_memory = False)])
    return df_firestore

def add_zero_text(text):
    if int(text) <= 9:
        return '0' + str(text)
    else:
        return str(text)

def restar_meses(x):
    if int(x[1]) >= int(x[0]):
        year_2 , year_1 = x[1][:4] , x[0][:4]
        mes_2 , mes_1 = x[1][4:] , x[0][4:]
        date_2 , date_1 = datetime.strptime(year_2 + ':' + mes_2, '%Y:%m'), datetime.strptime(year_1 + ':' + mes_1, '%Y:%m')
        num_months = (date_2.year - date_1.year) * 12 + (date_2.month - date_1.month)
        return int(num_months)

def clean_text(text):
    tildes = ['á', 'é', 'í', 'ó', 'ú']
    normal = ['a', 'e', 'i', 'o', 'u']
    text = text.lower()
    text = re.sub('[%s]' % re.escape(string.punctuation), ' ' , text)
    #text = re.sub('\w*\d\w*', ' ' , text)
    for j in range(len(tildes)):
        if tildes[j] in text:
            text = text.replace(tildes[j], normal[j])
    text = " ".join(text.split())
    text = text.strip()
    if (text[-2:] == ' i') | (text[-2:] == ' a' ) | (text[-2:] == ' e' ) | (text[-2:] == ' o' ):
        text = text[:-2]
    return text
# ====================================================================================================================================
def transform_whole_data_set(df_c):
    df = df_c.copy()
    df.dropna(subset=['VC_PERSONAL_REGIMEN_LABORAL', 'VC_PERSONAL_MATERNO', 'VC_PERSONAL_NOMBRES'], inplace= True)
    df['VC_PERSONAL_CARGO'] = df['VC_PERSONAL_CARGO'].fillna('Vacio')
    df['VC_PERSONAL_CARGO'] = df['VC_PERSONAL_CARGO'].apply(lambda x: clean_text(x))
    df['VC_PERSONAL_DEPENDENCIA'] = df['VC_PERSONAL_DEPENDENCIA'].fillna('Vacio')
    df['IN_PERSONAL_MES'] = df['IN_PERSONAL_MES'].apply(lambda x: add_zero_text(x))
    df['IN_PERSONAL_ANNO'] = df['IN_PERSONAL_ANNO'].astype('str')
    df['nombre completo'] = df['VC_PERSONAL_PATERNO'] + '_' + df['VC_PERSONAL_MATERNO'] + '_' + df['VC_PERSONAL_NOMBRES']
    df['nombre completo'] = df['nombre completo'].apply(lambda x: clean_text(x))
    df['MES_TRABAJADO'] = (df['IN_PERSONAL_ANNO'] +  df['IN_PERSONAL_MES'])
    df['FECHA_PAG'] = df['FEC_REG'].apply(lambda x: obtain_date(x).date().strftime("%Y%m"))
    df['atraso pago meses'] = df[['MES_TRABAJADO', 'FECHA_PAG']].apply(restar_meses, axis = 1)
    unique_personal = df['nombre completo'].unique()
    personal_map = {u: i for i, u in enumerate(unique_personal)}
    df['id'] = df['nombre completo'].map(personal_map)
    df['MO_PERSONAL_TOTAL'] = df['MO_PERSONAL_TOTAL'].astype('float')
    count = pd.pivot_table(df, values = 'MO_PERSONAL_TOTAL', index= ['id'], aggfunc='count')
    earnings = pd.pivot_table(df, values = 'MO_PERSONAL_TOTAL', index= ['id'], aggfunc=np.sum)
    earnings.columns = ['total recibido (S/.)']
    n_cargos = pd.pivot_table(df , values = 'VC_PERSONAL_CARGO', index = ['id'], aggfunc = lambda x: len(x.unique()))
    n_cargos.columns = ['n cargos']
    cargos = pd.pivot_table(df , values = 'VC_PERSONAL_CARGO', index = ['id'], aggfunc = lambda x: x.unique())
    cargos.columns = ['cargo']
    n_dependencias = pd.pivot_table(df, values = 'VC_PERSONAL_DEPENDENCIA', index = ['id'], aggfunc= lambda x: len(x.unique()))
    n_dependencias.columns = ['ndependencias']
    dependencias = pd.pivot_table(df, values = 'VC_PERSONAL_DEPENDENCIA', index = ['id'], aggfunc= lambda x: x.unique())
    dependencias.columns = ['dependencias']
    n_meses_trabajados = pd.pivot_table(df, values = 'MES_TRABAJADO', index = ['id'], aggfunc= lambda x: x.nunique())
    n_meses_trabajados.columns = ['meses trabajados']
    primer = pd.pivot_table(df, values = 'MES_TRABAJADO', index = ['id'], aggfunc= np.min)
    primer.columns = ['primer mes']
    last = pd.pivot_table(df, values = 'MES_TRABAJADO', index = ['id'], aggfunc= np.max)
    last.columns = ['ultimo mes']
    atraso  = pd.pivot_table(df, values = 'atraso pago meses', index = ['id'], aggfunc= np.mean)
    atraso.columns = ['atraso pago promedio(meses)']
    to_concat = df[['id', 'nombre completo']].drop_duplicates()
    to_concat.index = to_concat['id']
    new_df = pd.concat([to_concat, earnings, n_meses_trabajados, primer, last, atraso, n_cargos, cargos, n_dependencias, dependencias ], axis = 1)
    new_df['atraso pago promedio(meses)'] = new_df['atraso pago promedio(meses)'].apply(lambda x: round(x, 2))
    new_df['salario mensual promedio (S/.)'] = new_df['total recibido (S/.)']/new_df['meses trabajados']
    new_df['salario mensual promedio (S/.)'] = new_df['salario mensual promedio (S/.)'].apply(lambda x: round(x, 2))
    new_df['total recibido (S/.)'] = new_df['total recibido (S/.)'].apply(lambda x: round(x, 2))
    new_df.drop(['id'], axis = 1, inplace = True)
    columnas = new_df.columns.tolist()
    columnas = [columnas[0]] + columnas[-1:] + columnas[1:-1]
    new_df = new_df[columnas]
    new_df.sort_values(by=['salario mensual promedio (S/.)'], ascending= False, inplace= True)
    new_df = new_df[new_df['total recibido (S/.)'] > 0]
    return new_df

# ====================================================================================================================================
def last_trabajo(texto, type_array):
    if isinstance(texto, type_array):
        return texto[-1]
    else:
        return texto

def filter_cargos(df):
    cargo_personas = {}
    for cargo, personas in df['last cargo'].value_counts().items():
        if personas > 5:
            cargo_personas[cargo] = personas
    del cargo_personas["pensionista"]
    del cargo_personas["sincargo"]
    return cargo_personas

def default_value():
    return 0

def normalizar(x):
    global dict_mean, dict_std
    cargo = x[0]
    salario = x[1]
    mean = dict_mean[cargo]
    std = dict_std[cargo]
    if mean:
        if std == 0:
            return (salario - mean)
        else:
            return (salario - mean)/std
    else:
        return 'n < 5'

def obtain_mean(cargo):
    global dict_mean
    mean = dict_mean[cargo]
    return mean

def obtain_std(cargo):
    global dict_std
    std = dict_std[cargo]
    return std

def obtener_outlier(salario, threshold):
    if salario >= threshold:
        return 1
    else:
        return 0

def complete_df_no_data(df):
    type_array = type(np.array([5,4]))
    df.loc[:,'last cargo'] = df['cargo'].apply(lambda x: last_trabajo(x, type_array))
    df.loc[:,'normalize'] = ['No data']*len(df)
    df.loc[:, 'salario promedio (S/.)'] = ['No data']*len(df)
    df.loc[:, 'salario std'] = ['No data']*len(df)
    df.loc[:, '3σ_outlier'] = [0]*len(df)
    df.loc[:, '6σ_outlier'] = [0]*len(df)
    return df

def get_df_from_dict(dict):
    lista = []
    for i, v1 in dict.items():
        lista.append([i,v1])
    return lista

def produce_mean_std_df(cargo_personas, dict_mean, dict_std):
    df_muestra = pd.DataFrame(get_df_from_dict(cargo_personas), columns= ['cargo', 'muestra']).sort_values('cargo')
    df_muestra.index = range(len(df_muestra))
    df_mean = pd.DataFrame(get_df_from_dict(dict_mean), columns= ['cargo', 'promedio']).sort_values(by = ['cargo'])
    df_mean.index = range(len(df_muestra))
    df_std = pd.DataFrame(get_df_from_dict(dict_std), columns= ['cargo', 'desviacon']).sort_values(by = ['cargo'])
    df_std.index = range(len(df_muestra))
    df = pd.concat([df_muestra, df_mean, df_std], axis = 1)
    df = df.loc[:,~df.columns.duplicated()].sort_values(by = ['cargo'])
    df.to_csv(os.getcwd() + "/planilla_transform/mean_std_df.csv")
# ====================================================================================================================================
def outlier_detection(df_c):
    global dict_mean, dict_std
    type_array = type(np.array([5,4]))
    df = df_c.copy()
    df.loc[:,'last cargo'] = df['cargo'].apply(lambda x: last_trabajo(x, type_array))
    cargo_personas = filter_cargos(df)
    df_clean = df[df['last cargo'].isin(list(cargo_personas.keys()))]
    mean_std_pd = pd.pivot_table(df_clean, values = 'salario mensual promedio (S/.)', index = ['last cargo'], aggfunc= [np.mean, np.std])
    dict_mean = defaultdict(default_value, mean_std_pd['mean']['salario mensual promedio (S/.)'])
    dict_std = defaultdict(default_value, mean_std_pd['std']['salario mensual promedio (S/.)'])
    produce_mean_std_df(cargo_personas, dict_mean, dict_std)
    df_clean_c = df_clean.copy()
    df_clean_c.loc[:, 'normalize'] = df_clean_c[['last cargo', 'salario mensual promedio (S/.)']].apply(normalizar, axis = 1)
    df_clean_c.loc[:, 'salario promedio (S/.)'] = df_clean_c['last cargo'].apply(lambda x: obtain_mean(x))
    df_clean_c.loc[:, 'salario std'] = df_clean_c['last cargo'].apply(lambda x: obtain_std(x))
    #df_clean_c.loc[:, 'moderate_outlier'] = df_clean_c['normalize'].apply(lambda x:obtener_outlier(x, 2))
    df_clean_c.loc[:, '3σ_outlier'] = df_clean_c['normalize'].apply(lambda x:obtener_outlier(x, 3))
    df_clean_c.loc[:, '6σ_outlier'] = df_clean_c['normalize'].apply(lambda x:obtener_outlier(x, 6))
    index_no_data = list(set(list(df_c.index)).difference(set(list(df_clean_c.index))))
    df_no_data = df_c.loc[index_no_data]
    df_no_data = complete_df_no_data(df_no_data)
    df_clean_c = pd.concat([df_clean_c, df_no_data]).sort_index()
    return df_clean_c

def print_time(seconds, message):
    seconds = int(seconds)
    horas = seconds//3600
    minutos = seconds//60 - (horas*60)
    segundos = seconds - horas*3600 - minutos*60
    print(u"Takes: {} hours, {} minutes, {} seconds {}".format(horas, minutos, segundos, message))

print("********************         TRANSFORMING PLANILLA        ********************\n\n")
print("Loading..")
df_fire_c = obtener_df_de_firestore()
print("Transforming...")
t0 = time.time()
df_fire_transform = transform_whole_data_set(df_fire_c)
t1 = time.time()
print_time(t1 - t0, "to transformed the data.")
print("Outlier Detection...")
t2 = time.time()
df_fire_transform = outlier_detection(df_fire_transform)
outlier_3 = df_fire_transform[df_fire_transform['3σ_outlier'] == True]
t3 = time.time()
print_time(t3 - t2, "to obtained outliers")
print("Saving...")
df_fire_transform.to_csv(os.getcwd() + '/planilla_transform/planilla_transform.csv')
df_fire_transform.drop(["total recibido (S/.)", "atraso pago promedio(meses)", "last cargo",
    "normalize"], axis = 1, inplace = True)
df_fire_transform.to_csv(os.getcwd() + '/producto/planilla_perfil.csv')
outlier_3.to_csv(os.getcwd() + '/planilla_transform/outlier_3.csv')
print(u"Saved in {}".format(os.getcwd() + '/planilla_transform'))
print("********************")
