import json
import pandas as pd
from datetime import datetime, date
from tqdm import tqdm
import re
import time 
import os

# Load csvs
def load_csv():
    planilla = pd.read_csv(os.getcwd() + '/planilla_transform/planilla_transform.csv')
    planilla = planilla.rename(columns = {'id': 'id_planilla'})
    visitas = pd.read_csv(os.getcwd() + '/visitas_transform/visitantes_transformed.csv')
    visitas = visitas.rename(columns= {'id': 'id_visitas'})
    visitas_ministerios = pd.read_csv(os.getcwd() + '/visitas_ministerios_transform/visitantes_transformed.csv')
    visitas_ministerios = visitas_ministerios.rename(columns={'id':'id_visitas_min'})
    proveedores = pd.read_csv(os.getcwd() + '/proveedores/proveedores_transform.csv')
    proveedores = proveedores.rename(columns={'id':'id_proveedores'})
    return planilla, visitas, visitas_ministerios, proveedores

# Post transforms
def obtain_dnis(string):
    string = string.replace("'", "")
    string = string.strip('][')
    numbers = re.findall(r'\d+', string)
    numbers = [i for i in numbers if len(i) == 8]
    numbers = list(set(numbers))
    return numbers

def get_dni_list_len_one(lista):
    if (not isinstance(lista, str)) and list is not None and len(lista) != 0:
        #print(lista)
        return lista[0] 
    else:
        return lista

def dnis_more_rows(visitas_c):
    visitas = visitas_c.copy()
    more_dnis = visitas[visitas['len_documentos'] > 1.0].copy()
    duplicates = pd.DataFrame()
    for i in range(len(more_dnis)):
        #print(more_dnis.iloc[i])
        #print(i ,' : ', more_dnis.iloc[i]['len_documentos'])
        for j in range(more_dnis.iloc[i]['len_documentos']):
            df_to_add = pd.DataFrame(more_dnis.iloc[i]).T.copy()
            df_to_add['N_Documento_'] = more_dnis.iloc[i]['N_Documento_'][j]
            duplicates = pd.concat([duplicates, df_to_add])

    visitas = visitas[visitas['len_documentos'] <= 1]
    visitas = pd.concat([visitas, duplicates])
    visitas['N_Documento_'] = visitas['N_Documento_'].apply(lambda x: get_dni_list_len_one(x))
    return visitas

def get_dni_representantes(proveedor):
    dnis = []

    if not isinstance(proveedor['organos_nroDocumento'], float):
        dnis.extend(re.findall(r'\d+',proveedor['organos_nroDocumento']))
    if not isinstance(proveedor['representantes_nroDocumento'], float):
        dnis.extend(re.findall(r'\d+', proveedor['representantes_nroDocumento']))
    if not isinstance(proveedor['socios_dni'], float):
        dnis.extend(re.findall(r'\d+', proveedor['socios_dni']))
    if (proveedor['tipoEmpresa'] == 'persona natural sin negocio') or (proveedor['tipoEmpresa'] == 'persona natural con negocio'):
        dnis.append(str(proveedor['ruc'])[2:-1])
    else:
        return dnis
    dnis = list(set(dnis))
    return dnis

    
def encontrados_visitantes(proveedores, new_visitas):
    empresa_visitante_representante = pd.DataFrame()
    cont = 0
    for k in tqdm(range(len(proveedores))):
        #print(u"{}".format(k))
        dnis = get_dni_representantes(proveedores.iloc[k])
        #print(dnis)
        if len(dnis) > 0:
            for i in dnis:
                visitante_representante_df = new_visitas[new_visitas['N_Documento_'] == i]
                if len(visitante_representante_df) > 0:
                    proveedor_df = pd.DataFrame(proveedores.iloc[k]).T.copy()
                    proveedor_df.index = [cont]
                    #print(u"{} {}".format(k,i))
                    visitante_representante_df.index = [cont]
                    #print(dnis)
                    #print(u"k: {}, proveedor ruc: {}".format(k , proveedores.iloc[k]['ruc']))
                    #print(i)
                    cont += 1
                    to_add = pd.concat([proveedor_df, visitante_representante_df], axis = 1)
                    empresa_visitante_representante = pd.concat([empresa_visitante_representante, to_add])

    return empresa_visitante_representante

def esta_en(x):
    try:
        lista = re.findall(r'\d+', x[0])
        if x[1] in lista:
            return 1
        else:
            return 0
    except:
        return 0

def ruc_igual(x):
    #try:
    if str(x[0])[2:-1] == x[1]:
        return 1
    else:
        return 0
    #except:
    #    return 0

def visitantes_empresas(visitas, visitas_ministerios):
    # Solo visitas
    visitas['N_Documento_'] = visitas['N_Documento'].apply(lambda x: obtain_dnis(x))
    visitas['len_documentos'] = visitas['N_Documento_'].apply(lambda x: len(x))
    new_visitas = dnis_more_rows(visitas)
    print('Visitantes congreso/presidencia\n')
    empresa_visitante_representante = encontrados_visitantes(proveedores, new_visitas)
    # Visitas ministerios
    visitas_ministerios['N_Documento_'] = visitas_ministerios['N_Documento'].apply(lambda x: obtain_dnis(x))
    visitas_ministerios['len_documentos'] = visitas_ministerios['N_Documento_'].apply(lambda x: len(x))
    new_visitas_ministerios = dnis_more_rows(visitas_ministerios)
    print('Visitantes ministerios\n')
    empresa_visitante_mins_representante = encontrados_visitantes(proveedores, new_visitas_ministerios)

    visitantes = pd.concat([empresa_visitante_representante, empresa_visitante_mins_representante])
    visitantes = visitantes.sort_values(by = ['ruc'])
    visitantes.index = list(range(len(visitantes)))

    visitantes['es_organo'] = visitantes[['organos_nroDocumento', 'N_Documento_']].apply(esta_en, axis = 1)
    visitantes['es_representante'] = visitantes[['representantes_nroDocumento', 'N_Documento_']].apply(esta_en, axis = 1)
    visitantes['es_socio'] = visitantes[['socios_dni', 'N_Documento_']].apply(esta_en, axis = 1)
    visitantes['es_persona_natural'] = visitantes[['ruc', 'N_Documento_']].apply(ruc_igual, axis = 1)
    return visitantes

def get_fecha(x, s = '-'):
    if (x != None) or (x!=''):
        #print(datetime.strptime(x[:10], '%Y-%m-%d').date())
        try:
            return datetime.strptime(x[:10], '%Y'+ s + '%m' + s + '%d').date()
        except:
            return None
    else:
        return None

def obtain_months_past(x):
    return (x[1].year - x[0].year) * 12 + (x[1].month - x[0].month)

def my_value(number):
    return ("{:,}".format(number))

def add_info_despues_visita(visitantes_c):
    visitantes = visitantes_c.copy()
    f = open(os.getcwd() + '/proveedores/proveedores_load.txt', 'r')
    lines = f.readlines()
    data_df = pd.DataFrame()
    for linea in tqdm(lines):
        item = json.loads(linea)
        if item is not None:
            data = {}
            df_values = pd.DataFrame(item['experiencia_seace'])
            df_values = pd.DataFrame(item['experiencia_seace'])
            if (len(df_values) > 0) & (str(item['proveedor']['numeroRuc']) in map(str, list(visitantes['ruc']))):
                df_values['fecha'] = df_values['fecProgTerm'].apply(lambda x: get_fecha(x))
                primer_dia = get_fecha(visitantes[visitantes['ruc'] == int(item['proveedor']['numeroRuc'])]['primer_dia'].iloc[0])
                despues_visita = df_values[df_values['fecha'] >= primer_dia]
                if len(despues_visita) > 0:
                    data['ruc_'] = str(item['proveedor']['numeroRuc'])
                    data['ganados_despues_visita'] = len(despues_visita)
                    data['monto_ganado_despues_visita'] = round(despues_visita['montoOrigen'].sum(),1)
                    fecha_min = despues_visita['fecha'].apply(pd.to_datetime).min().date()
                    fecha_max = despues_visita['fecha'].apply(pd.to_datetime).max().date()
                    if fecha_max > date.today():
                        fecha_max = date.today()
                    meses_activo = max(obtain_months_past([fecha_min,fecha_max]),1)
                    data['monto_promedio_despues_visita'] = round(despues_visita['montoOrigen'].sum()/meses_activo,1)
                else:
                    data['ruc_'] = str(item['proveedor']['numeroRuc'])
                    data['ganados_despues_visita'] = 0
                    data['monto_ganado_despues_visita'] = 0.0
                    data['monto_promedio_despues_visita'] = 0.0
            data_frame = pd.DataFrame.from_dict(data, orient = 'index').T
            data_df = pd.concat([data_df, data_frame])
    return data_df

def add_info_faltante(data_after_c, visitantes_c):
    data_after = data_after_c.copy()
    visitantes = visitantes_c.copy()
    data_after_rucs = set(list(data_after['ruc_'].unique()))
    visitas_unique_ruc = set(map(str, list(visitantes['ruc'].unique())))
    falta_info = list(visitas_unique_ruc.difference(data_after_rucs))
    for faltantes in falta_info:
        data = {}
        data['ruc_'] = faltantes
        data['ganados_despues_visita'] = 0
        data['monto_ganado_despues_visita'] = 0.0
        data['monto_promedio_despues_visita'] = 0.0
        data_frame = pd.DataFrame.from_dict(data, orient = 'index').T
        data_after = pd.concat([data_after, data_frame])
    return data_after

def print_time(seconds, message):
    seconds = int(seconds)
    horas = seconds//3600
    minutos = seconds//60 - (horas*60)
    segundos = seconds - horas*3600 - minutos*60
    print(u"Takes: {} hours, {} minutes, {} seconds {}".format(horas, minutos, segundos, message))


if __name__ == '__main__':
    print("********************         VISITANTES EMPRESARIOS        ********************\n\n")
    t0 = time.time()
    print("Loading ...")
    planilla, visitas, visitas_ministerios, proveedores = load_csv()
    print('Encontrando visitantes empresarios ...')
    visitantes = visitantes_empresas(visitas, visitas_ministerios)
    data_after = add_info_despues_visita(visitantes)
    print('Agregando información importante de las empresas ganadoras ...')
    data_after = add_info_faltante(data_after, visitantes)
    visitantes.index = visitantes['ruc'].apply(lambda x:str(x))
    data_after.index = data_after['ruc_'].apply(lambda x:str(x))
    encontrados = visitantes.join(data_after, how = 'outer')
    encontrados.to_csv(os.getcwd() + '/encontrados/visitantes_empresarios.csv')
    #columnas_drop = ['seace_desCatObj_keys', 'seace_desCatObj_values','seace_desEstContProv_keys',\
    #                'seace_desEstContProv_values', 'id_visitas', 'Tipo_Documento' ,\
    #                'N_Documento', 'id_visitas_min']
    #visitantes_clean = encontrados.drop(columnas_drop, axis = 1)
    #visitantes.to_csv(os.getcwd() + '/encontrados/para_presentar.csv')
    t1 = time.time()
    print_time(t1 - t0, 'para obtener visitantes empresarios e info de las empresas.')
    print("********************")