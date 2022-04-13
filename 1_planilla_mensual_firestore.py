import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from google.cloud import firestore
import time
import os
from os import listdir
from os.path import isfile, join
from datetime import timedelta, date, datetime
from collections import defaultdict


# Este codigo solo lee los datos del firestore y los pone en local.
# Solo hace transformaciones menores.
# El ciclo es:
    # 1. Se conecta al firestore.
    # 2. Lee los ministerios de entidades.txt
    # 3. Obtiene las fechas (en meses) que faltan por leer. En el caso de que no haya ningún .csv en planilla_mensual, leeré todas las fechas.
    # 4. Lee los meses del firestore y los guarda.

def daterange(date1, date2):
    for n in range(int ((date2 - date1).days) + 1):
        yield date1 + timedelta(n)

# Crea una lista donde se tienen las fechas como string que tienen que ser revisados si existen en el firestore.
def fechas_ini_today():
    start_dt = date(2021, 1, 1)
    end_dt = date.today()
    fechas = []
    for dt in daterange(start_dt, end_dt):
        #fechas.append(dt.strftime("%Y-%m-%d").replace("-", ""))
        fechas.append(dt.strftime("%Y-%m-%d"))
    return fechas

def add_zero_to_int(num: int) -> str:
    if num > 9:
        return str(num)
    else:
        return '0' + str(num)

def dates_months_in_dict():
    month_dict = defaultdict(lambda: 0)
    fechas = fechas_ini_today()
    for dates in fechas:
        year = datetime.strptime(dates, "%Y-%m-%d").year
        month = datetime.strptime(dates, "%Y-%m-%d").month
        if month_dict[str(year) + add_zero_to_int(month)] == 0:
            month_dict[str(year) + add_zero_to_int(month)] = []
        else:
            month_dict[str(year) + add_zero_to_int(month)].append(dates.replace('-', ''))

    return list(month_dict.keys())

def obtener_files():
    directorio = os.getcwd()
    directorio_files = directorio + '/planilla_mensual'
    onlyfiles = [f for f in listdir(directorio_files) if isfile(join(directorio_files, f))]
    return onlyfiles

def test_prev_existence(collection_name, item_name, collection_name_2, document_ ):
    db = firestore.Client()
    doc_ref = db.collection(collection_name).document(item_name).collection(collection_name_2).document(document_)
    doc = doc_ref.get()
    if doc.exists:
        return (True, doc.to_dict())
    else:
        return (False, 2)

def fechas_func():
    meses_a_completar = dates_months_in_dict()
    onlyfiles = obtener_files()
    fechas_save = [i[:6] for i in onlyfiles]
    fechas_no_save = list(set(meses_a_completar).difference(set(fechas_save)))
    fechas_no_save.extend(sorted(meses_a_completar)[-4:])
    fechas_no_save = list(set(fechas_no_save))
    return sorted(fechas_no_save)
    
def obtain_data_from_date(item_name, collection_name, entidades):
    daily_monthly = pd.DataFrame()
    list_documentos = list(range(1, 2000))              # Arg4
    key_save = False
    for ministerio in tqdm(entidades, desc = "Downloading data from firestore"):
        for documento in list_documentos:
            #print(u"Mes: {}, Ministerio: {}, Documento: {}".format(item_name, ministerio, documento), end = "")
            booleano, doc= test_prev_existence(collection_name, item_name, ministerio, str(documento))
            if booleano == True:
                daily_monthly = pd.concat([daily_monthly, pd.DataFrame(doc).T])
                key_save = 1
                #print(u", Data: {}".format(len(daily_df)))
                #df_transformed = add_data_to_old_data(df_transformed, df_new_loaded)
                #print(u"Agregado")
            else:
                #print(u", Data: No data")
                break
    if key_save > 0 :
        daily_monthly.to_csv(u"planilla_mensual/{}_{}.csv".format(item_name, collection_name))
    return daily_monthly

def print_time(seconds):
    seconds = int(seconds)
    horas = seconds//3600
    minutos = seconds//60 - (horas*60)
    segundos = seconds - horas*3600 - minutos*60
    print(u"Takes: {} hours, {} minutes, {} seconds donwload the data".format(horas, minutos, segundos))

if __name__ == '__main__':
    print("********************         LOADING PLANILLA        ********************\n\n")
    load_dotenv('google.env')                           # Recordar que se debe tener un file google.env con los keys
    collection_name = "PlanillaEjecutivo"               # Arg1

    f = open('entidades.txt', 'r')                      # Entidades
    entidades = f.readlines()
    entidades = [i[:-1] for i in entidades]             # Arg2

    fechas = fechas_func()
    t3 = time.time()
    for fecha in fechas:
        t0 = time.time()
        print(u"Downloading {}...".format(fecha))
        monthly_df = obtain_data_from_date(fecha, collection_name, entidades)
        t1 = time.time()
        print(u"Rows downloaded: {}".format(len(monthly_df)))
        print_time(t1 - t0)
        print('\n===========================================================\n')
    t4 = time.time()
    print_time(t4 - t0)
    print("********************")