from datetime import timedelta, date
import pandas as pd
import tqdm
import string
import re
from datetime import datetime
from collections import defaultdict
from tqdm import tqdm
import time
import os
from os import listdir
from os.path import isfile, join
from dotenv import load_dotenv
from google.cloud import firestore

def test_prev_existence(collection_name, item_name):
    
    db = firestore.Client()
    doc_ref = db.collection(collection_name).document(item_name)

    doc = doc_ref.get()
    
    if doc.exists:
        #print(True)
        if len(doc.to_dict()) == 0:
            return (False, 2)
        else:
            return (True, doc.to_dict())
    else:
        return (False, 2)

def daterange(date1, date2):
    for n in range(int ((date2 - date1).days) + 1):
        yield date1 + timedelta(n)

def obtener_files():
    directorio = os.getcwd()
    directorio_files = directorio + '/visitas_ministerios_mensual'
    onlyfiles = [f for f in listdir(directorio_files) if isfile(join(directorio_files, f))]
    months = [month[:-4] for month in onlyfiles]
    return months

# Crea una lista donde se tienen las fechas como string que tienen que ser revisados si existen en el firestore.
def fechas_func():
    start_dt = date(2021, 7, 28)
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
    fechas = fechas_func()
    for dates in fechas:
        year = datetime.strptime(dates, "%Y-%m-%d").year
        month = datetime.strptime(dates, "%Y-%m-%d").month
        if month_dict[str(year) + '_' + add_zero_to_int(month)] == 0:
            month_dict[str(year) + '_' + add_zero_to_int(month)] = []
        else:
            month_dict[str(year) + '_' + add_zero_to_int(month)].append(dates.replace('-', ''))

    return month_dict

def clean_text(text):
    tildes = ['á', 'é', 'í', 'ó', 'ú']
    normal = ['a', 'e', 'i', 'o', 'u']
    text = text.lower()
    text = re.sub('[%s]' % re.escape(string.punctuation), ' ' , text)
    for j in range(len(tildes)):
        if tildes[j] in text:
            text = text.replace(tildes[j], normal[j])
    text = " ".join(text.split())
    text = text.strip()
    if (text[-2:] == ' i') | (text[-2:] == ' a' ) | (text[-2:] == ' e' ) | (text[-2:] == ' o' ):
        text = text[:-2]
    return text

def obtain_data_from_date(collection_names ,item_name):
    daily = pd.DataFrame()
    for institucion in collection_names:
        booleano, doc= test_prev_existence(institucion, item_name)
        if booleano == True:
            daily_inst = pd.DataFrame(doc).T
            fecha_column = [date(int(item_name[:4]) , int(item_name[4:6]), int(item_name[6:]))]*len(daily_inst)
            daily_inst['fecha'] = fecha_column
            daily_inst['entidad'] = institucion
            daily = pd.concat([daily, daily_inst])
    
    daily.index = list(range(len(daily)))
    daily.fillna(' ', inplace= True)
    if len(daily) > 0:
        columnas = ['Visitante', 'Oficina', 'Institucion', 'Visitado', 'Cargo', 'Motivo']
        for columna in columnas:
            daily[columna] = daily[columna].apply(lambda x: clean_text(x))
    return daily

def print_time(seconds, message):
    seconds = int(seconds)
    horas = seconds//3600
    minutos = seconds//60 - (horas*60)
    segundos = seconds - horas*3600 - minutos*60
    print(u"Takes: {} hours, {} minutes, {} seconds {}".format(horas, minutos, segundos, message))

def load_from_firestore():
    global collection_names, month_dict
    t3 = time.time()
    meses_faltantes = list(set(list(month_dict.keys())).difference(set(obtener_files())))
    meses_faltantes.append(list(month_dict.keys())[-1])
    meses_faltantes = sorted(list(set(meses_faltantes)))
    print("Meses a obtener denuevo:" , meses_faltantes)
    for month in tqdm(meses_faltantes):
        t0 = time.time()    
        month_csv = pd.DataFrame()
        for fecha in month_dict[month]:
            print(u"Fecha {}".format(fecha))
            pd_daily = obtain_data_from_date(collection_names, fecha)
            month_csv = pd.concat([month_csv, pd_daily])
        print(month, len(month_csv))
        month_csv.index = list(range(len(month_csv)))
        month_csv.to_csv(u"visitas_ministerios_mensual/{}.csv".format(month))
        t1 = time.time()
        print_time(t1 - t0, "to donwload the data .") 
    t4 = time.time()
    print_time(t4 - t3, "to donwload all the data.")

if __name__ == '__main__':
    print("********************         LOADING VISITAS MINISTERIOS        ********************\n\n")
    global collection_names, month_dict
    load_dotenv('google.env')
    f = open('entidades_visitas.txt', 'r')                      # Entidades
    entidades = f.readlines()
    collection_names = [i[:-1] for i in entidades]             # Arg2
    month_dict = dates_months_in_dict()
    load_from_firestore()
    print("********************")