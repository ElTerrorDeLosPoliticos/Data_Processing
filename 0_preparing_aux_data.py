import os
import pandas as pd
import json
from os import listdir
from os.path import isfile, join

def obtener_files(carpeta):
    directorio = os.getcwd()
    directorio_files = directorio + "/" + carpeta
    onlyfiles = [f for f in listdir(directorio_files) if isfile(join(directorio_files, f))]
    return onlyfiles

def read_excel_convert_to_csv():
    carpeta = "CONOSCE_CONTRATOS"
    onlyfiles = obtener_files(carpeta)
    for file in onlyfiles:
        contrato = pd.read_excel(os.getcwd() + "/" + carpeta + "/" + file, header = 1)
        contrato.index.name = 'id'
        contrato.to_csv(os.getcwd() + "/conosce_contratos_csv/" + file[:-5] + '.csv')

def obtener_contrato_fecha_suscripcion():
    files = obtener_files("conosce_contratos_csv")
    dict_contratos_fecha = {}
    for file in files:
        df_aux = pd.read_csv(os.getcwd() + "/conosce_contratos_csv/" + file, low_memory = False)
        df_aux = df_aux[['N_COD_CONTRATO', 'FECHA_SUSCRIPCION_CONTRATO']]
        df_aux.drop_duplicates(inplace= True)
        dict_contratos_fecha.update(dict(zip(df_aux.N_COD_CONTRATO, df_aux.FECHA_SUSCRIPCION_CONTRATO)))

    with open('contratos_fechas.json', 'w') as outfile:
        json.dump(dict_contratos_fecha, outfile, indent= 1)

if __name__ == '__main__':
    read_excel_convert_to_csv()
    obtener_contrato_fecha_suscripcion()