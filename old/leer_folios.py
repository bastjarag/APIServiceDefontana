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

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from base64 import b64decode

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



def adjust_key_length(key):
    # Asegurarse de que la clave tenga una longitud de 24 bytes
    if len(key) > 24:
        return key[:24]  # Truncar si la longitud es mayor a 24
    else:
        return key.ljust(24, '\0')  # Rellenar con '\0' (null byte) si es menor

def decrypt(enc_data, key):
    key = adjust_key_length(key)
    enc = b64decode(enc_data)
    iv = enc[:AES.block_size]
    ct = enc[AES.block_size:]
    cipher = AES.new(key.encode(), AES.MODE_CBC, iv)
    pt = unpad(cipher.decrypt(ct), AES.block_size)
    return pt.decode('utf-8')

# conexion a la base de datos con variables de entorno
def conexion_base_datos(): 
    decryption_key = 'oe2023'  # Reemplazar con tu clave real

    # Obtener y desencriptar las variables de entorno
    database_enc = os.environ.get('apiservicedefontana_database')
    user_enc = os.environ.get('apiservicedefontana_user')
    host_enc = os.environ.get('apiservicedefontana_host')
    password_enc = os.environ.get('apiservicedefontana_password')
    port_enc = os.environ.get('apiservicedefontana_port')

    '''print(f"Encrypted database: {database_enc}")
    print(f"Encrypted user: {user_enc}")
    print(f"Encrypted host: {host_enc}")
    print(f"Encrypted password: {password_enc}")
    print(f"Encrypted port: {port_enc}")'''


    if not all([database_enc, user_enc, host_enc, password_enc, port_enc]):
        raise Exception("Faltan variables de entorno para la conexión a la base de datos.")

    database = decrypt(database_enc, decryption_key)
    user = decrypt(user_enc, decryption_key)
    host = decrypt(host_enc, decryption_key)
    password = decrypt(password_enc, decryption_key)
    port = int(decrypt(port_enc, decryption_key))

    try:
        conexion = psycopg2.connect(database=database, user=user, host=host, password=password, port=port)
        return conexion
    except Exception as e:
        print("Error de Conexion BD: {}".format(e))
        fecha_actual = datetime.now().strftime("%d-%m-%Y %H:%M:%S")#.strftime("%Y-%m-%d %H:%M:%S")
        with open(ruta_archivo, 'a') as archivo:
            archivo.write("FECHA: {} || Archivo: {} || Error de conexion BD: {}\n".format(fecha_actual,nombre_de_este_archivo, str(e)))

        
# conexion a la base de datos deprecado.
'''def conexion_base_datos(): 
    
    # Datos para la conexion
    #database = "API_MOTOR" #bd de pruebas.........
    database = "API_DATA"
    user = "bjara"
    #host = "172.28.0.1"
    host = '192.168.124.60' #con vpn
    password = "jHxB@sD48*"
    port = 5432
    
    

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
'''

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
    