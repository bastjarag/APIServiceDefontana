import json
from datetime import datetime, timedelta
import psycopg2
import requests
#import servicemanager
import os
import sys
import getpass
#import subprocess
import csv
#from time import sleep
import time

##configuración:
#log txt:
###CAMBIAR NOMBRE ARCHIVO SEGÚN DONDE ESTÉ EJECUTANDO
nombre_de_este_archivo = 'obtener_facturas_ventas.py'###CAMBIAR NOMBRE ARCHIVO SEGÚN DONDE ESTÉ EJECUTANDO
###CAMBIAR NOMBRE ARCHIVO SEGÚN DONDE ESTÉ EJECUTANDO 
fecha_actual_str = datetime.now().strftime("%d-%m-%Y %H:%M:%S")#.strftime("%Y-%m-%d %H:%M:%S")
nombre_archivo = os.path.abspath(sys.argv[0])# Obtener el nombre del archivo actual (incluyendo la ruta completa)
nombre_archivo_con_extension = os.path.basename(nombre_archivo)# Extraer solo el nombre del archivo con la extensión
nombre_archivo_sin_extension = os.path.splitext(os.path.basename(nombre_archivo))[0]

nombre_log= 'log_defontana_'+nombre_archivo_sin_extension+'.txt'
usuario_actual = getpass.getuser()
# si se cambia el server DEBE cambiarse el nombre
if usuario_actual == 'WIN-869S8VCILTL$': #estoy en el servidor windows server para correr el servicio de python...
    ruta_archivo = os.path.join("C:\\Program Files\\Oenergy\\APIServiceDefontana", nombre_log)
else:
    ruta_archivo = os.path.join("C:\\Program Files\\Oenergy\\APIServiceDefontana", nombre_log)




# conexion a la base de datos
def conexion_base_datos(): 
    
    # Datos para la conexion
    #database = "API_MOTOR" #bd de pruebas.........
    database = "API_DATA"
    user = "bjara"
    #host = "172.28.0.1"
    host = '192.168.124.60' #con vpn
    password = "jHxB@sD48*"
    port = 5432
    
    '''
       # base de datos oEnergy
    database = "API_MOTOR"
    user = "fcordova"
    host = "172.28.0.1"
    password = "xLp-5791(+)"
    port = 5432
    '''

    try:
        validation = psycopg2.connect(database=database,
                                      user=user,
                                      host=host,
                                      password=password,
                                      port=port)
        return validation
    except Exception as e:
        print("Error de Conexion BD: {}".format(e))
        fecha_actual = datetime.now().strftime("%d-%m-%Y %H:%M:%S")#.strftime("%Y-%m-%d %H:%M:%S")
        with open(ruta_archivo, 'a') as archivo:
            archivo.write("FECHA: {} || Archivo: {} || Error de conexion BD: {}\n".format(fecha_actual,nombre_de_este_archivo, str(e)))


def obtener_caf_por_empresa(_token): #con el token identifico en qué empresa estoy.
    url = "https://api.defontana.com/api/Sale/GetCafSummary?documentIds=33"
    
    payload = {}
    headers = {
    'Authorization': 'Bearer {}'.format(_token)
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    print ("response:")
    print (response)
    return response
    