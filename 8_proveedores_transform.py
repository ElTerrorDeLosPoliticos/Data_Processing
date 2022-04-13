import os
import pickle
import json
import pandas as pd
from datetime import datetime, date
from tqdm import tqdm
import re
import string
import time 

def get_fecha(x, s = '-'):
    if (x != None) or (x!=''):
        #print(datetime.strptime(x[:10], '%Y-%m-%d').date())
        try:
            return datetime.strptime(x[:10], '%Y'+ s + '%m' + s + '%d').date()
        except:
            return None
    else:
        return None

def get_fecha_2(x, s = '-'):
    if (x != None) or (x!=''):
        #print(datetime.strptime(x[:10], '%Y-%m-%d').date())
        try:
            return datetime.strptime(x, '%d'+ s + '%m' + s + '%Y').date()
        except:
            return None
    else:
        return None

def obtain_months_past(x):
    return (x[1].year - x[0].year) * 12 + (x[1].month - x[0].month)

def get_numb(x):
    return re.findall(r'\d+', x)[0]

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

# Experiencia_rnp
def get_data_from_experiencia_rnp(item, data):
    df_values = pd.DataFrame(item['experiencia_rnp'])
    if len(df_values) > 0:
        df_values['fec_Contrato'] = df_values['fecContrato'].apply(lambda x: get_fecha(x))
        df_values['fec_Culminacion'] = df_values['fecCulminacion'].apply(lambda x: get_fecha(x))
        df_values['duracion_meses'] = max(df_values[['fec_Contrato', 'fec_Culminacion']].apply(obtain_months_past, axis= 1) ,1)
        df_values['ampliacion_'] = df_values['ampliacion'].apply(lambda x: get_numb(x))

        # Retorno 
        data['rnp_registros'] = len(df_values)
        data['rnp_gasto_total'] = round(df_values['mtoExp'].sum(),1)
        data['rnp_gasto_promedio_meses_ejecucion'] = round(df_values['mtoExp'].sum()/(df_values['duracion_meses'].sum()),1)
        data['rnp_fecha_min'] = df_values['fec_Contrato'].apply(pd.to_datetime).min().date()
        data['rnp_fecha_max'] = df_values['fec_Culminacion'].apply(pd.to_datetime).max().date()
        data['rnp_meses_activo'] = obtain_months_past([data['rnp_fecha_min'], data['rnp_fecha_max']])
        data['rnp_ampliacion'] = round(pd.to_numeric(df_values['ampliacion_']).sum(),1)
    else:
        data['rnp_registros'] = None
        data['rnp_gasto_total'] = None
        data['rnp_gasto_promedio_meses_ejecucion'] = None
        data['rnp_fecha_min'] = None
        data['rnp_fecha_max'] = None
        data['rnp_meses_activo'] = None
        data['rnp_ampliacion'] = None
    return data

# Experiencia_seace
def get_data_from_experiencia_seace(item, data):
    df_values = pd.DataFrame(item['experiencia_seace'])
    if len(df_values) > 0:
        df_values['fecha'] = df_values['fecProgTerm'].apply(lambda x: get_fecha(x))
        fecha_min = df_values['fecha'].apply(pd.to_datetime).min().date()
        if fecha_min > date.today():
            fecha_min = date.today()
        fecha_max = df_values['fecha'].apply(pd.to_datetime).max().date()
        if fecha_max > date.today():
            fecha_max = date.today()
        meses_activo = max(obtain_months_past([fecha_min,fecha_max]),1)
        # add to dict
        data['seace_registros'] = len(df_values)
        data['seace_gasto_total'] = round(df_values['montoOrigen'].sum(),1)
        data['seace_gasto_promedio_mensual'] = round(data['seace_gasto_total']/meses_activo, 1) # Total / meses activos
        data['seace_desCatObj_keys'] = '_._'.join(list(df_values.desCatObj2.value_counts().to_dict().keys()))
        data['seace_desCatObj_values'] = '_._'.join(map(str, list(df_values.desCatObj2.value_counts().to_dict().values())))
        data['seace_desEstContProv_keys'] = '_._'.join(list(df_values.desEstContProv.value_counts().to_dict().keys()))
        data['seace_desEstContProv_values'] = '_._'.join(map(str, list(df_values.desEstContProv.value_counts().to_dict().values())))
        data['seace_fecha_min'] = fecha_min
        data['seace_fecha_max'] = fecha_max
        data['seace_meses_activo'] = meses_activo
    else:
        data['seace_registros'] = None
        data['seace_gasto_total'] = None
        data['seace_gasto_promedio_mensual'] = None
        data['seace_desCatObj_keys'] = None
        data['seace_desCatObj_values'] = None
        data['seace_desEstContProv_keys'] = None
        data['seace_desEstContProv_values'] = None
        data['seace_fecha_min'] = None
        data['seace_fecha_max'] = None
        data['seace_meses_activo'] = None
    return data

# OrganosAdm
def get_data_from_organosAdm(item, data):
    df_values = pd.DataFrame(item['organosAdm'])
    if len(df_values) > 0:
        df_values['apellidosNomb_'] = df_values['apellidosNomb'].apply(lambda x: clean_text(x))
        dict_organos = dict(zip(df_values['apellidosNomb_'].values, df_values['nroDocumento'].values))
        # add to dict
        data['n_organos'] = len(df_values)
        data['organos_nomb_apell'] = '_._'.join(list(dict_organos.keys()))
        data['organos_nroDocumento'] = '_._'.join(list(dict_organos.values()))
    else:
        data['n_organos'] = None
        data['organos_nomb_apell'] = None
        data['organos_nroDocumento'] = None
    return data

# Proveedor
def get_data_proveedor(item, data):
    if len(item['proveedor']) > 0:
        if item['proveedor']['clcTexto'] != 'null':
            data['clc'] = float(item['proveedor']['clcTexto'][2:].replace(',', ''))
            data['cmc'] = float(item['proveedor']['cmcTexto'][2:].replace(',', ''))
        else:
            data['clc'] = None
            data['cmc'] = None
        data['tipoEmpresa'] = clean_text(item['proveedor']['tipoEmpresa'])
    else:
        data['clc'] = None
        data['cmc'] = None
        data['tipoEmpresa'] = None
    return data

# Representantes
def get_data_representantes(item, data):
    df_values = pd.DataFrame(item['representantes'])
    if len(df_values) > 0:
        df_values['apellidosNomb_'] = df_values['razonSocial'].apply(lambda x: clean_text(x))
        dict_representante = dict(zip(df_values['apellidosNomb_'].values, df_values['nroDocumento'].values))
        data['representantes_nomb_apell'] = '_._'.join(list(dict_representante.keys()))
        data['representantes_nroDocumento'] = '_._'.join(list(dict_representante.values()))
    else:
        data['representantes_nomb_apell'] = None
        data['representantes_nroDocumento'] = None
    return data

# Sanciones
def get_data_sanciones(item, data):
    df_values = pd.DataFrame(item['sanciones'])
    if len(df_values) > 0:
        df_values['fechaFin_'] = df_values['fechaFin'].apply(lambda x: get_fecha_2(x , '/'))
        df_values['fechaIni_'] = df_values['fechaIni'].apply(lambda x: get_fecha_2(x, '/'))
        df_values['meses_sancionado'] = df_values[['fechaIni_', 'fechaFin_']].apply(obtain_months_past, axis = 1)
        data['n_sanciones'] = len(df_values)
        data['meses_sancionado'] = df_values['meses_sancionado'].sum()
    else:
        data['n_sanciones'] = None
        data['meses_sancionado'] = None
    return data

# Socios
def get_data_socios(item, data):
    df_values = pd.DataFrame(item['socios'])
    if len(df_values) > 0:
        df_values['nombre'] = df_values['razonSocial'].apply(lambda x: clean_text(x))
        data['n_socios'] = len(df_values)
        data['socios'] = '_._'.join(df_values['nombre'])
        data['fecha_ingreso_socio'] = '_._'.join(df_values['fechaIngreso'])
        data['socios_dni'] = '_._'.join(df_values['nroDocumento'])
        data['%_acciones'] = '_._'.join(map(str, df_values['porcentajeAcciones']))
    else:
        data['n_socios'] = None
        data['socios'] = None
        data['fecha_ingreso_socio'] = None
        data['socios_dni'] = None
        data['%_acciones'] = None

    return data

def get_data_completa_linea(linea, rucs):
    try:
        linea = linea[:-1]
        data = {}
        item = json.loads(linea)
        ruc = item['proveedor']['numeroRuc']
        data['ruc'] = ruc
        data['Razon_social'] = rucs[ruc]
        #data = get_data_from_experiencia_rnp(item, data)
        data = get_data_from_experiencia_seace(item, data)
        data = get_data_from_organosAdm(item, data) 
        data = get_data_proveedor(item, data)
        data = get_data_representantes(item, data)
        data = get_data_sanciones(item, data)
        data = get_data_socios(item, data)
        data_frame = pd.DataFrame.from_dict(data, orient = 'index').T
        #data_frame.index = list([cont])
    except:
        data_frame = pd.DataFrame()
    return data_frame


def print_time(seconds, message):
    seconds = int(seconds)
    horas = seconds//3600
    minutos = seconds//60 - (horas*60)
    segundos = seconds - horas*3600 - minutos*60
    print(u"Takes: {} hours, {} minutes, {} seconds {}".format(horas, minutos, segundos, message))

def load_transform_save_data():
    #with open('rucs_proveedores.json', 'r', encoding='utf-8') as f:
    with open('rucs_proveedores_2021_2022.json', 'r', encoding='utf-8') as f:
        rucs = json.load(f, encoding='utf-8')
    f = open(os.getcwd() + '/proveedores/proveedores_load.txt', 'r')
    lines = f.readlines()
    data_complete = pd.DataFrame()
    for line in tqdm(lines):
        data_line = get_data_completa_linea(line, rucs)
        data_complete = pd.concat([data_complete, data_line])
    data_complete.index = list(range(len(data_complete)))
    data_complete.index.name = 'id'
    print(u"Saving data in {}".format(os.getcwd() + '/proveedores/proveedores.csv'))
    data_complete.to_csv(os.getcwd() + '/proveedores/proveedores.csv')
    columas_exportar = ['ruc', 'Razon_social', 'tipoEmpresa',
     'seace_registros', 'seace_gasto_total', 'seace_gasto_promedio_mensual',
     'seace_fecha_min', 'seace_fecha_max', 'n_sanciones', 'meses_sancionado',
     'organos_nomb_apell', 'organos_nroDocumento', 'representantes_nomb_apell',
     'representantes_nroDocumento', 'socios', 'socios_dni']
    data_complete = data_complete[columas_exportar]
    print(u"Saving data in {}".format(os.getcwd() + '/producto/proveedores_perfil.csv'))
    data_complete.to_csv(os.getcwd() + '/producto/proveedores_perfil.csv')

if __name__ == '__main__':
    print("********************         TRANSFORMING PROVEEDORES        ********************\n\n")
    t0 = time.time()
    print("Loading and Transforming ...")
    load_transform_save_data()
    t1 = time.time()
    print_time(t1 - t0, 'to transform the data.')
    print("********************")