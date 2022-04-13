import os
import pandas as pd
from tqdm import tqdm
from os.path import isfile, join
from os import listdir
import time 

# 1. Planilla_buscador
# 2. Planilla_perfil (esto se hace en el 2_planilla_tra....py )
# 3. Visitantes_buscador.csv  (presidencial/congreso + ministerios)
# 4. Visitantes_perfil (presidencial/congreso + ministerios)
# 5. Proveedores_perfil (se hace en 8_provee....py)
# 6. Visitantes_empresarios (se hace en 10_encontra....py)

def obtener_files(string):
    directorio = os.getcwd()
    directorio_files = directorio + "/" + string
    onlyfiles = [f for f in listdir(directorio_files) if (isfile(join(directorio_files, f))) and (f[-4:] == ".csv" ) ]
    return onlyfiles

def obtener_df_de_firestore_planilla(string = "planilla_mensual", 
                nombre_guardar = "planilla_buscador"):
    onlyfiles = obtener_files("planilla_mensual")
    df_firestore = pd.DataFrame()
    for i in tqdm(onlyfiles, desc = "Downloading " + string):
        mensual_df = pd.read_csv(os.getcwd() + "/" + string + "/" + i, low_memory= False)
        if len(mensual_df) > 0:
            df_firestore = pd.concat([df_firestore, mensual_df])
    df_firestore.index = list(range(len(df_firestore)))
    df_firestore.index.name = "registro"
    df_firestore.drop(["Unnamed: 0"], axis = 1, inplace= True)
    columnas_ordenadas = ['VC_PERSONAL_NOMBRES',  'VC_PERSONAL_PATERNO',  'VC_PERSONAL_MATERNO',
    'ENTIDAD', 'VC_PERSONAL_DEPENDENCIA',  'VC_PERSONAL_REGIMEN_LABORAL',
    'VC_PERSONAL_CARGO', 'VC_PERSONAL_RUC_ENTIDAD', 'PK_ID_PERSONAL',
    'FEC_REG', 'IN_PERSONAL_ANNO', 'IN_PERSONAL_MES', 'MO_PERSONAL_TOTAL',
    'MO_PERSONAL_HONORARIOS', 'MO_PERSONAL_REMUNERACIONES', 'MO_PERSONAL_GRATIFICACION',
    'MO_PERSONAL_INCENTIVO', 'MO_PERSONAL_OTROS_BENEFICIOS', 'VC_PERSONAL_OBSERVACIONES']
    df_firestore = df_firestore[columnas_ordenadas]
    df_firestore.to_csv(os.getcwd() + "/producto/" + nombre_guardar + ".csv")
    
def obtener_df_de_firestore_visitas(string):
    onlyfiles = obtener_files(string)
    df_firestore = pd.DataFrame()
    for i in tqdm(onlyfiles, desc = "Downloading " + string):
        mensual_df = pd.read_csv(os.getcwd() + '/' + string + '/' + i, low_memory = False)
        if len(mensual_df) > 0:
            df_firestore = pd.concat([df_firestore, mensual_df])
    df_firestore.drop(['Unnamed: 0'], axis = 1, inplace= True)
    df_firestore.dropna(subset=['Visitante'], inplace= True)
    df_firestore.index = list(range(len(df_firestore)))
    return df_firestore

def obtener_visitantes_todos():
    df_visitas_pre_con = obtener_df_de_firestore_visitas("visitas_mensuales")
    df_visitas_min = obtener_df_de_firestore_visitas("visitas_ministerios_mensual")
    df_visitas_todos = pd.concat([df_visitas_pre_con , df_visitas_min])
    #del df_visitas_pre_con, df_visitas_min
    df_visitas_todos.index = list(range(len(df_visitas_todos)))
    columnas_ordenadas = ['fecha', 'Hora_Ingreso', "Hora_Salida", "Motivo",
    "Visitante", "Tipo_Documento", "N_Documento", "Institucion", "Visitado",
    "entidad", "Cargo", "Oficina", "Observacion"]
    df_visitas_todos = df_visitas_todos[columnas_ordenadas]
    df_visitas_todos.to_csv(os.getcwd() + '/producto/visitantes_buscador.csv')

def obtener_visitantes_transform_todos():
    df_visitante_trans_pre_con = pd.read_csv(os.getcwd() + "/visitas_transform/visitantes_transformed.csv")
    df_visitante_trans_min = pd.read_csv(os.getcwd() + "/visitas_ministerios_transform/visitantes_transformed.csv")
    df_visitante_trans_pre_con.sort_values(by=['id'], inplace = True)
    len_trans_pre_con = len(df_visitante_trans_pre_con)
    df_visitante_trans_pre_con.index = list(range(len_trans_pre_con))
    df_visitante_trans_min.sort_values(by=['id'], inplace=True)
    len_trans_min = len(df_visitante_trans_min)
    df_visitante_trans_min.index = list(range(len_trans_pre_con + 1, len_trans_pre_con + len_trans_min + 1 ))
    df_visitante_trans_min['id'] += len_trans_pre_con + 1
    df_visitas_transform_todos = pd.concat([df_visitante_trans_pre_con, df_visitante_trans_min])
    df_visitas_transform_todos.to_csv(os.getcwd() + '/producto/visitantes_perfil.csv')

def print_time(seconds, message):
    seconds = int(seconds)
    horas = seconds//3600
    minutos = seconds//60 - (horas*60)
    segundos = seconds - horas*3600 - minutos*60
    print(u"Takes: {} hours, {} minutes, {} seconds {}".format(horas, minutos, segundos, message))

if __name__ == '__main__':
    t0 = time.time()
    print("********************         PRODUCTOS        ********************\n\n")
    obtener_df_de_firestore_planilla()      #1
    obtener_visitantes_todos()              #3
    obtener_visitantes_transform_todos()    #4
    t1 = time.time()
    print_time(t1 - t0, " to generate the .csvs.")