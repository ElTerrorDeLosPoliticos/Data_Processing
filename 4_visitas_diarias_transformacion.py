import pandas as pd
import numpy as np
from datetime import datetime
pd.set_option('display.max_columns', 30)
from tqdm import tqdm
import os
from os import listdir
from os.path import isfile, join
import matplotlib.pyplot as plt
import time
from collections import defaultdict
from sklearn.cluster import DBSCAN
from sklearn.decomposition import PCA

def obtener_df_de_firestore():
    onlyfiles = obtener_files()
    df_firestore = pd.DataFrame()
    for i in tqdm(onlyfiles):
        mensual_df = pd.read_csv(os.getcwd() + '/visitas_mensuales/' + i, low_memory = False)
        if len(mensual_df) > 0:
            df_firestore = pd.concat([df_firestore, mensual_df])
    df_firestore.drop(['Unnamed: 0'], axis = 1, inplace= True)
    df_firestore.dropna(subset=['Visitante'], inplace= True)
    df_firestore.index = list(range(len(df_firestore)))
    return df_firestore

def obtener_files():
    directorio = os.getcwd()
    directorio_files = directorio + '/visitas_mensuales'
    onlyfiles = [f for f in listdir(directorio_files) if (isfile(join(directorio_files, f))) and (f[-4:] == ".csv" ) ]
    return onlyfiles

def obtain_date(fecha):
    if fecha != None and isinstance(fecha, str):
        try:
            dt_1 = datetime.strptime(fecha, '%H:%M')
        except:
            dt_1 = datetime.strptime(fecha, '%d/%m/%Y %H:%M')
    else:
        return 'vacio'
    return dt_1

def restar_horas(x):
    # Revisa si no es null y si es str
    if (x[0] != None) and (isinstance(x[0], str)):
        try:
            dt_1 = datetime.strptime(x[0], '%H:%M')
        except:
            dt_1 = datetime.strptime(x[0], '%d/%m/%Y %H:%M')
    else:
        dt_1 = 0
    # Revisa si no es null y si es str
    if (x[1] != None) and (isinstance(x[1], str)):
        try:
            dt_2 = datetime.strptime(x[1], '%H:%M')
        except:
            dt_2 = datetime.strptime(x[1], '%d/%m/%Y %H:%M')
    else:
        dt_2 = 0

    if (dt_1 == 0) or (dt_2 == 0):
        return 0
    else:
        delta = dt_1 - dt_2
        delta = delta.seconds/60
        return delta

# Transformacion visitantes
def transform_visitantes(df_c):
    df = df_c.copy()
    df['Duración_reunion(min)'] = df[['Hora_Salida', 'Hora_Ingreso']].apply(restar_horas, axis = 1)
    # Hard-code personas que aparecen como 2 nombres distintos:
    # Esto va a ser importante arreglar por el paso 9.
    # Buscar una forma de automatizar
    df.loc[df['Visitante'] =='castillo sologuren jorge',('Visitante')] = 'castillo sologuren jorge alberto'
    df.loc[df['Visitante'] =='surco hachiri benardino julio',('Visitante')] = 'surco hachiri bernardino julio'    
    df.loc[df['Visitante'] =='martinez moron alain cesar',('Visitante')] = 'martinez moron alan cesar'  
    unique_personal = df['Visitante'].unique()
    personal_map = {u: i for i, u in enumerate(unique_personal)}
    df['id'] = df['Visitante'].map(personal_map)
    df['reunion_sin_tiempo'] = df['Duración_reunion(min)'].apply(lambda x: 0 if x!=0 else 1) 
    fechas = pd.pivot_table(df, values= 'fecha', index = ['id'], aggfunc= lambda x: len(x.unique()))
    fechas.columns = ['fechas']
    df['fecha'] = df['fecha'].apply(lambda x : datetime.strptime(x, '%Y-%m-%d'))
    recurrencia = pd.pivot_table(df, values = 'Duración_reunion(min)', index= ['id'], aggfunc='count')
    recurrencia.columns = ['#_Visitas']
    tiempo_total = pd.pivot_table(df, values = 'Duración_reunion(min)', index= ['id'], aggfunc=np.sum)
    tiempo_total.columns = ['Tiempo_reuniones(min)']
    n_visitados = pd.pivot_table(df , values = 'Visitado', index = ['id'], aggfunc = lambda x: len(x.unique()))
    n_visitados.columns = ['#_Visitados']
    visitados = pd.pivot_table(df , values = 'Visitado', index = ['id'], aggfunc = lambda x: x.unique())
    visitados.columns = ['Visitados']
    tipo_documento = pd.pivot_table(df, values = 'Tipo_Documento', index= ['id'], aggfunc= lambda x: x.unique())
    documento = pd.pivot_table(df, values = 'N_Documento', index= ['id'], aggfunc= lambda x: x.unique())
    cargos = pd.pivot_table(df , values = 'Cargo', index = ['id'], aggfunc = lambda x: x.unique())
    cargos.columns = ['Cargos']
    n_oficinas = pd.pivot_table(df , values = 'Oficina', index = ['id'], aggfunc = lambda x: len(x.unique()))
    n_oficinas.columns = ['#_oficinas']
    oficinas = pd.pivot_table(df , values = 'Oficina', index = ['id'], aggfunc = lambda x: x.unique())
    oficinas.columns = ['Oficinas']
    min_dia = pd.pivot_table(df, values= 'fecha', index = ['id'], aggfunc= np.min)
    min_dia.columns = ['primer_dia']
    max_dia = pd.pivot_table(df, values = 'fecha', index = ['id'], aggfunc= np.max)
    max_dia.columns = ['ultimo_dia']
    reunion_sin_tiempo = pd.pivot_table(df, values = 'reunion_sin_tiempo', index= ['id'], aggfunc= np.sum)
    entidades = pd.pivot_table(df, values = 'entidad', index = ['id'], aggfunc= lambda x: x.unique())
    entidades.columns = ['entidades']
    new_df = df[['id','Visitante']].copy()
    new_df.index = new_df['id']
    new_df.drop_duplicates(inplace= True)
    to_df = pd.concat([new_df, fechas, recurrencia,tiempo_total, min_dia, max_dia, reunion_sin_tiempo, n_visitados, visitados, tipo_documento, documento, cargos, n_oficinas, oficinas, entidades], axis = 1)
    to_df.drop(['id'], axis = 1, inplace = True)
    to_df['tiempo_reunion/n_fechas (h)'] = to_df['Tiempo_reuniones(min)']/to_df['fechas']/60.0
    to_df['tiempo_reunion/n_fechas (h)'] = to_df['tiempo_reunion/n_fechas (h)'].apply(lambda x : round(x, 2))
    to_df.sort_values(by = ['fechas'], ascending = False, inplace = True)
    columnas = list(to_df.columns)
    columnas = columnas[:4] + [columnas[-1]] + columnas[4:-1]
    to_df = to_df[columnas]
    return to_df

# Transformacion visitados
def transform_visitados(df_c):
    df = df_c.copy()
    df['Duración_reunion(min)'] = df[['Hora_Salida', 'Hora_Ingreso']].apply(restar_horas, axis = 1)
    unique_personal = df['Visitado'].unique()
    personal_map = {u: i for i, u in enumerate(unique_personal)}
    df['id'] = df['Visitado'].map(personal_map)
    df['reunion_sin_tiempo'] = df['Duración_reunion(min)'].apply(lambda x: 0 if x!=0 else 1) 
    fechas = pd.pivot_table(df, values= 'fecha', index = ['id'], aggfunc= lambda x: len(x.unique()))
    fechas.columns = ['fechas']
    df['fecha'] = df['fecha'].apply(lambda x : datetime.strptime(x, '%Y-%m-%d'))
    recurrencia = pd.pivot_table(df, values = 'Duración_reunion(min)', index= ['id'], aggfunc='count')
    recurrencia.columns = ['#_Visitas']
    tiempo_total = pd.pivot_table(df, values = 'Duración_reunion(min)', index= ['id'], aggfunc=np.sum)
    tiempo_total.columns = ['Tiempo_reuniones(min)']
    n_visitados = pd.pivot_table(df , values = 'Visitante', index = ['id'], aggfunc = lambda x: len(x.unique()))
    n_visitados.columns = ['#_Visitantes']
    #visitados = pd.pivot_table(df , values = 'Visitante', index = ['id'], aggfunc = lambda x: x.unique())
    #visitados.columns = ['Visitados']
    #tipo_documento = pd.pivot_table(df, values = 'Tipo_Documento', index= ['id'], aggfunc= lambda x: x.unique())
    #documento = pd.pivot_table(df, values = 'N_Documento', index= ['id'], aggfunc= lambda x: x.unique())
    cargos = pd.pivot_table(df , values = 'Cargo', index = ['id'], aggfunc = lambda x: x.unique())
    cargos.columns = ['Cargos']
    n_oficinas = pd.pivot_table(df , values = 'Oficina', index = ['id'], aggfunc = lambda x: len(x.unique()))
    n_oficinas.columns = ['#_oficinas']
    oficinas = pd.pivot_table(df , values = 'Oficina', index = ['id'], aggfunc = lambda x: x.unique())
    oficinas.columns = ['Oficinas']
    min_dia = pd.pivot_table(df, values= 'fecha', index = ['id'], aggfunc= np.min)
    min_dia.columns = ['primer_dia']
    max_dia = pd.pivot_table(df, values = 'fecha', index = ['id'], aggfunc= np.max)
    max_dia.columns = ['ultimo_dia']
    reunion_sin_tiempo = pd.pivot_table(df, values = 'reunion_sin_tiempo', index= ['id'], aggfunc= np.sum)
    entidades = pd.pivot_table(df, values = 'entidad', index = ['id'], aggfunc= lambda x: x.unique())
    entidades.columns = ['entidades']
    new_df = df[['id','Visitado']].copy()
    new_df.index = new_df['id']
    new_df.drop_duplicates(inplace= True)
    to_df = pd.concat([new_df, fechas, recurrencia,tiempo_total, min_dia, max_dia, reunion_sin_tiempo, n_visitados, cargos, n_oficinas, oficinas, entidades], axis = 1)
    to_df.drop(['id'], axis = 1, inplace = True)
    to_df['tiempo_reunion/n_fechas (h)'] = to_df['Tiempo_reuniones(min)']/to_df['fechas']/60.0
    to_df['tiempo_reunion/n_fechas (h)'] = to_df['tiempo_reunion/n_fechas (h)'].apply(lambda x : round(x, 2))
    to_df.sort_values(by = ['fechas'], ascending = False, inplace = True)
    columnas = list(to_df.columns)
    columnas = columnas[:4] + [columnas[-1]] + columnas[4:-1]
    to_df = to_df[columnas]
    return to_df

def outlier_values(num):
    if num == -1:
        return 1
    else:
        return 0

def outlier_detection(df, tipo, nombre, eps=0.05, min_samples=20):
    if tipo == 'visitante':
        columnas_para_pca = ['#_Visitas', 'Tiempo_reuniones(min)', 'tiempo_reunion/n_fechas (h)', 'reunion_sin_tiempo', '#_Visitados', '#_oficinas']
    elif tipo == 'visitado':
        columnas_para_pca = ['#_Visitas', 'Tiempo_reuniones(min)', 'tiempo_reunion/n_fechas (h)', 'reunion_sin_tiempo', '#_Visitantes', '#_oficinas']
    df_outliers = df[columnas_para_pca].copy()
    df_outliers_norm = (df_outliers - df_outliers.min())/(df_outliers.max() - df_outliers.min())
    X = df_outliers_norm[columnas_para_pca].values
    # PCA
    pca = PCA(n_components=2)
    X2D = pca.fit_transform(X)
    # DBSCAN
    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    dbscan.fit(X2D)

    # Grafico
    plt.figure(figsize=(12, 8))
    plt.subplot(111)
    plot_dbscan(dbscan, X2D, size=100)
    save_fig(nombre + '_' + str(min_samples))
    plt.close()

    df['outlier_dbscan'] = dbscan.labels_
    df['outlier_dbscan'] = df['outlier_dbscan'].apply(lambda x : outlier_values(x))
    return df

def save_fig(fig_id, tight_layout=True, fig_extension="png", resolution=300):
    IMAGES_PATH = os.getcwd() + '/imagenes/visitas'
    path = os.path.join(IMAGES_PATH, fig_id + "." + fig_extension)
    print("Saving figure", fig_id , '...')
    if tight_layout:
        plt.tight_layout()
    plt.savefig(path, format=fig_extension, dpi=resolution)

def plot_dbscan(dbscan, X, size, show_xlabels=True, show_ylabels=True):
    core_mask = np.zeros_like(dbscan.labels_, dtype=bool)
    core_mask[dbscan.core_sample_indices_] = True
    anomalies_mask = dbscan.labels_ == -1
    non_core_mask = ~(core_mask | anomalies_mask)

    cores = dbscan.components_
    anomalies = X[anomalies_mask]
    non_cores = X[non_core_mask]
    
    plt.scatter(cores[:, 0], cores[:, 1],
                c=dbscan.labels_[core_mask], marker='o', s=size, cmap="Paired")
    plt.scatter(cores[:, 0], cores[:, 1], marker='*', s=20, c=dbscan.labels_[core_mask])
    plt.scatter(anomalies[:, 0], anomalies[:, 1],
                c="r", marker="x", s=100)
    plt.scatter(non_cores[:, 0], non_cores[:, 1], c=dbscan.labels_[non_core_mask], marker=".")
    if show_xlabels:
        plt.xlabel("$x_1$", fontsize=14)
    else:
        plt.tick_params(labelbottom=False)
    if show_ylabels:
        plt.ylabel("$x_2$", fontsize=14, rotation=0)
    else:
        plt.tick_params(labelleft=False)
    plt.title("eps={:.2f}, min_samples={}".format(dbscan.eps, dbscan.min_samples), fontsize=14)

def print_time(seconds, message):
    seconds = int(seconds)
    horas = seconds//3600
    minutos = seconds//60 - (horas*60)
    segundos = seconds - horas*3600 - minutos*60
    print(u"Takes: {} hours, {} minutes, {} seconds {}".format(horas, minutos, segundos, message))


if __name__ == '__main__':
    print("********************         TRANSFORMING VISITAS        ********************\n\n")
    print("Loading..")
    time.sleep(1.0)
    df = obtener_df_de_firestore()
    print("Transforming...")
    t0 = time.time()
    df_transform_visitantes = transform_visitantes(df)
    df_transform_visitados = transform_visitados(df)
    t1 = time.time()
    print_time(t1 - t0, "to transformed the data.")
    print("Outliers...")
    t2 = time.time()
    df_transform_visitantes = outlier_detection(df_transform_visitantes, tipo = 'visitante', nombre = 'visitante_dbscan_plot', min_samples=150)
    df_transform_visitados = outlier_detection(df_transform_visitados, tipo = 'visitado', nombre = 'visitado_dbscan_plot')
    t3 = time.time()
    print_time(t3-t2, "to obtain the outliers.")
    print("Saving...")
    df_transform_visitantes.to_csv(os.getcwd() + '/visitas_transform/visitantes_transformed.csv')
    df_transform_visitados.to_csv(os.getcwd() + '/visitas_transform/visitados_transformed.csv')
    print("Finished!!!")
    print("********************")