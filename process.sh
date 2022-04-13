#!/usr/bin/env bash

# Creamos carpetas donde se guardarán los datos
mkdir producto
mkdir encontrados
mkdir imagenes
mkdir planilla_mensual
mkdir planilla_transform
mkdir proveedores
mkdir visitas_mensuales
mkdir visitas_ministerios_mensual
mkdir visitas_ministerios_transform
mkdir visitas_transform

if [[ -f "entidades.txt" ]];
then
    echo "Documento con entidades existe. CONTINUA"
else
    echo "Documento con entidades NO existe"
    break
fi

if [[ -f "entidades_visitas.txt" ]];
then
    echo "Documento con entidades de ministerios existe. CONTINUA"
else
    echo "Documento con entidades de ministerios NO existe"
    break
fi


# Total process of loading from firestore or local , transform

# ********************          PLANILLA        ********************
# Descargamos la planilla desde firestore
echo "1/10"
python3 1_planilla_mensual_firestore.py
# Transformamos planilla y obtenemos outliers
echo "2/10"
python3 2_planilla_transformacion.py

# ********************          VISITAS         ********************
# Descargamos visitas desde firestore
echo "3/10"
python3 3_visitas_diarias_full_firestore.py
# Transformamos las visitas y obtenemos outliers
echo "4/10"
python3 4_visitas_diarias_transformacion.py

# ********************          VISITAS MINISTERIOS         ********************
# Descargamos visitas desde firestore
echo "5/10"
python3 5_visitas_ministerios_full_firestore.py
#Transformamos las visitas y obtenemos outliers
echo "6/10"
python3 6_visitas_diarias_mins_transformacion.py

# ********************          PROVEEDORES GANADORES         ********************
# Descargamos los proveedores.
# En este caso, la cantidad de proveedores que vamos a buscar esta fija. Tenemos que buscar una forma de actualizarla.
# Esta parte demora más
if [[ -f "rucs_proveedores_2021_2022.json" ]];
then
echo "El archivo .json con proveedores existe"
else
echo "Se necesita tener un .json con todas las empresas a buscar"
fi

if [[ -f "./proveedores/proveedores_load.txt" ]];
then
echo "proveedores_load.txt existe, no es necesario recargarlo"
else
echo "7/10"
python3 7_proveedores_load.py
fi
# Transformamos la info de proveedores para que nos sea más util
echo "8/10"
python3 8_proveedores_transform.py

# ********************          CROSS VISITANTES-PROVEEDORES         ********************
# Se obtienen los visitantes que pertenecen a una empresa
echo "9/10"
python3 9_producto_csv_export.py

echo "10/10"
python3 10_encontrar_visitantes_empresarios.py