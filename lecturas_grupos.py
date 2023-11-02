import os
import sys
import time
import psycopg2
from threading import Thread
import json
import pandas as pd
import requests
from datetime import datetime, timedelta

# Configuración txt LOG
log_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), "."))

#MÉTODOS EN COMÚN:
#VALORES SEGÚN Tabla tbl_status_log
INICIO_OK = 1
ENDPOINTS_OK = 2

RESPUESTA_OK = 3
RESPUESTA_ALERT = 4
RESPUESTA_FAIL = 5

INSERTADOS_OK = 6
INSERTADOS_ALERT = 7
INSERTADOS_FAIL = 8

FIN_OK = 9
FIN_ALERT = 10
FIN_FAIL = 11
#FIN VALORES SEGÚN TABLA de BD.

def connection_database():
    #database = "API_MOTOR" #bd de pruebas....
    database = "API_DATA"
    user = "bjara"
    #host = "172.28.0.1"
    host = '192.168.124.60' #con vpn
    password = "jHxB@sD48*"
    port = 5432
    
    try:
        conexion = psycopg2.connect(database=database, user=user, host=host, password=password, port=port)
        #print(f"{datetime.now().strftime('%H:%M:%S %d/%m/%Y')} - Conexión a BD exitosa")
        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} {datetime.now().strftime('%d/%m/%Y')} - Conexión a BD exitosa")
        return conexion
    except Exception as e:
        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} {datetime.now().strftime('%d/%m/%Y')} - Error de conexión a BD: {str(e)}")
        return None
    
def obtener_lecturas_grupo_actual(conexion, _id_gpo):
    cursor = conexion.cursor()
    query = """
    SELECT l.id_lectura, l.id_emp, l.max_reintentos, l.url, l.endpoint, l.item_por_pagina,
            l.numero_pagina, l.token, g.id_gpo
        FROM public.tbl_lecturas_defontana l
        INNER JOIN tbl_grupos_lecturas g ON l.id_gpo = g.id_gpo
        WHERE l.activo = true
            and g.id_gpo = {}
            and l.endpoint = 'get_caf'
        ORDER BY l.id_emp, g.id_gpo, l.id_lectura;
    """.format(_id_gpo)
    ####QUITAR TESTEO DE LINEA ID GPO 15 and g.id_gpo = 15

    #print ("query: "+query)
    cursor.execute(query)
    conexion.commit()

    # Obtener los nombres de las columnas del resultado
    column_names = [desc[0] for desc in cursor.description]

    # Guardar los resultados en una lista con los mismos nombres de columna
    x_por_leer = [dict(zip(column_names, row)) for row in cursor.fetchall()]

    return x_por_leer

def obtener_lecturas_por_grupo(conexion):

    cursor = conexion.cursor()
    query = """
     SELECT l.id_lectura, l.id_emp, l.max_reintentos, l.url, l.endpoint, l.item_por_pagina,
            l.numero_pagina, l.token, g.id_gpo
        FROM public.tbl_lecturas_defontana l
        INNER JOIN tbl_grupos_lecturas g ON l.id_gpo = g.id_gpo
        WHERE l.activo = true
        and l.endpoint = 'get_caf'
         
        ORDER BY l.id_emp, g.id_gpo, l.id_lectura;
    """
    #####
    #QUITAR TESTEO DE ID GPO 15 and g.id_gpo = 15


    cursor.execute(query)
    lecturas_por_grupo = {}

    for row in cursor.fetchall():
        id_grupo_lectura = row[-1]  # Última columna es id_grupo_lectura
        lectura = {}
        for desc, value in zip(cursor.description, row):
            lectura[desc.name] = value  # Usar desc.name como clave en el diccionario
        # Crear la lista si aún no existe y agregar la lectura
        if id_grupo_lectura not in lecturas_por_grupo:
            lecturas_por_grupo[id_grupo_lectura] = []
        lecturas_por_grupo[id_grupo_lectura].append(lectura)

    return lecturas_por_grupo

### escribe en txt del grupo:
def escribir_archivo_log(id_grupo_lectura, lecturas):
    #escribo en el log del grupo cada configuracion de lectura.
    log_filename = os.path.join(log_directory, f"log_grupo_lectura_{id_grupo_lectura}.txt")
    with open(log_filename, 'a') as log_file:
        ahora = datetime.now()
        cadena_fecha = ahora.strftime("%H:%M:%S.%f %d/%m/%Y")
        log_file.write(f"\n--- Registro de lecturas a las {cadena_fecha} ---\n")
        log_file.write(f"Cantidad de Endpoints: {len(lecturas)}\n")

        for i, lectura in enumerate(lecturas, start=1):
            log_file.write("Fecha y hora: "+cadena_fecha)
            
            log_file.write(
                f"\nLectura {i}:\n"
                f"\tId Lectura: {lectura['id_lectura']}\n"
                f"\tId Empresa: {lectura['id_emp']}\n"
                f"\tEndpoint: {lectura['endpoint']}\n"
                f"\tUrl: {lectura['url']}\n"
                f"\tItem por página: {lectura['item_por_pagina']}\n"
                f"\tMax Reintentos: {lectura['max_reintentos']}\n"
            )

def manejar_grupo(id_grupo):
    primera_ejecucion = True

    while True:
        ahora = datetime.now()

        if primera_ejecucion:
            # Para la primera ejecución, calcula el tiempo hasta el próximo minuto exacto
            hora_ejecucion = (ahora + timedelta(minutes=1)).replace(second=0, microsecond=0)
            primera_ejecucion = False
        else:
            # Para las ejecuciones subsiguientes, sigue el patrón basado en el ID del grupo
            minutos_hasta_el_proximo_intervalo = id_grupo - (ahora.minute % id_grupo)
            hora_ejecucion = (ahora + timedelta(minutes=minutos_hasta_el_proximo_intervalo)).replace(second=0, microsecond=0)

        segundos_hasta_el_proximo_intervalo = (hora_ejecucion - ahora).total_seconds()

        # Duerme la mayoría del tiempo
        time.sleep(segundos_hasta_el_proximo_intervalo)

        # Ahora ejecuta el proceso
        procesar_grupo_obtener_lecturas(id_grupo)
        print(f"Ejecutado proceso para el grupo {id_grupo} a las {hora_ejecucion.strftime('%H:%M:%S')}")

def procesar_factura_venta(lectura, ruta_archivo, ID_LOG):
    print ("a")

def procesar_caf(lectura, ruta_archivo, ID_LOG):
    print ("entre al procesar caf")
    id_lectura = lectura['id_lectura']
    id_emp = lectura['id_emp']
    url = lectura['url']
    endpoint = lectura['endpoint']
    token = lectura['token']
    

    payload = {}
    headers = {
    'Authorization': 'Bearer {}'.format(token)
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    print ('response antes de.. '+ str(response.json()))
    #return response
    # Verificar si la respuesta es válida y es un JSON
    if response.status_code == 200:
        data = response.json()
        if 'itemsCaf' in data:
            conexion = connection_database()  # Asumiendo que tienes una función para conectarte a la base de datos
            if not conexion:
                print("Error al conectar a la base de datos")
                return None

            for item in data['itemsCaf']:
                new_item = {
                    'id_emp': id_emp,
                    'fch_dato': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                new_item.update(item)
                
                # Inserta el item modificado en la base de datos
                resultado = insertar_caf_en_bd(conexion, new_item)
                if resultado == -1:
                    print(f"Error al insertar el item: {new_item}")

            ID_ESTATUS_DETA = INICIO_OK

            FCH_INICIO_LECT_GENE = datetime.now().strftime("%d-%m-%Y %H:%M:%S") # fch_dato en tbl_log

            CANT_LECT_GENE = 1 #hardcodeado, deberia ser if itemcaf 1 a 1.

            ID_LOG_DETA = ID_LOG #ya venia la id log, por ende no deberia volver a calcular.
            #print ("id_log",ID_LOG_DETA)

            ID_LECTURA_DETA = id_lectura

            ID_ESTATUS_DETA = ENDPOINTS_OK #LECTURAS DE METODOS A LEER OBTENIDAS        
            MENSAJE_COMPLETO_GEN = ''

            CANT_REINTENTOS_DETA = 0 #hardcodeado, debo mejorarlo.
            FCH_INICIO_LECT_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 

            CANT_READ_DETA = CANT_LECT_GENE #A ?
            CANT_INSERT_DETA = resultado

            FCH_DATO_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 
            FCH_FIN_LECT_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            
            ID_ESTATUS_DETA = FIN_OK

            #print ("CANT_READ_DETA ", str(items_totales))
            #print ("CANT_INSERT_DETA ", str(facturas_insertadas))

            insert_log_detalle_ok = insertar_datos_log_detalle_defontana(conexion, ID_LOG_DETA, ID_ESTATUS_DETA, FCH_DATO_DETA, FCH_INICIO_LECT_DETA, FCH_FIN_LECT_DETA, CANT_READ_DETA, CANT_INSERT_DETA, ID_LECTURA_DETA, CANT_REINTENTOS_DETA )
            print ("insert log det ok: ", str(insert_log_detalle_ok))
            #OK_LECT_GENE += 1     
            OK_LECT_GENE = 1 #hardcodeado?

            #if FAIL_LECT_GENE == CANT_LECT_GENE:
            #    COMENT_LECT_GENE = "API CON FALLA TOTAL"
            FCH_FIN_LECT_GENE = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            actualizar_log(cone, ID_LOG_DETA, FCH_INICIO_LECT_GENE, FCH_FIN_LECT_GENE, CANT_LECT_GENE, OK_LECT_GENE, FAIL_LECT_GENE, COMENT_LECT_GENE)
             

            conexion.close()
        else:
            print("La respuesta no contiene la clave 'itemsCaf'")
    else:
        print("El código de estado no es 200")

def insertar_caf_en_bd(conexion, item):
    cursor = conexion.cursor()
    try:
        result = cursor.callproc("fn_defontana_insertar_caf", [
            item['id_emp'],
            item['fch_dato'],
            item['tipoDocumento'],
            item['descripcion'],
            item['cantidadFoliosDisponibles'],
            item['ultimoFolioDisponible'],
            item['primerFolioDeCafActivos'],
            item['ultimoFolioUsado'],
            item['consumoPromedioMensual'],
            item['modeloFacturaElectronica'],
            item['mensajeError']
        ])
        conexion.commit()
        return result[0]  # Retorna el resultado de la función (1 para éxito, -1 para error)
    except Exception as e:
        print(f"Error al insertar en la base de datos: {str(e)}")
        return None
    finally:
        cursor.close()

def procesar_lectura(lectura, ruta_archivo):
#def procesar_lectura(lectura, ruta_archivo)#, ID_LOG):

    
    conexion = connection_database()

    tipo_lectura = lectura['endpoint']  # Asumiendo que 'endpoint' contiene la información de tipo de factura

    
    if tipo_lectura == "facturas_ventas":        
        # Inserta un registro en tbl_log y obtén el ID_LOG
        ID_LOG = insertar_datos_log(conexion)
        procesar_factura_venta(lectura, ruta_archivo, ID_LOG)#print ()
    
    elif tipo_lectura == "get_caf":  
        # Inserta un registro en tbl_log y obtén el ID_LOG
        ID_LOG = insertar_datos_log_caf(conexion)
        procesar_caf(lectura, ruta_archivo, ID_LOG)
    
    else:   
        print(f"Tipo de factura desconocido, id_lectura:  {lectura['id_lectura']}")

        with open(ruta_archivo, "a") as archivo:                   
            archivo.write( f"Tipo de lectura desconocido, id_lectura:  {lectura['id_lectura']}")

def procesar_grupo_obtener_lecturas(id_grupo):
    log_filename = os.path.join(log_directory, f"log_grupo_lectura_{id_grupo}.txt")
    ahora = datetime.now()
    cadena_fecha = ahora.strftime("%H:%M:%S.%f %d/%m/%Y")

    try:
        conexion = connection_database()

        if conexion:
            
            #print ("ID LOG "+str(ID_LOG))
            #ID_LOG = 999
            #revisar en bd para que no arroje -1 never.
            
            lecturas_grupo_actual = obtener_lecturas_grupo_actual(conexion, id_grupo)

            # escribo en el log cada lectura que haré secuencial.
            escribir_archivo_log(id_grupo, lecturas_grupo_actual)

            # Proceso cada lectura y evaluo si es COMPRA o VENTA.
            for lectura in lecturas_grupo_actual:
                procesar_lectura(lectura, log_filename)#, ID_LOG)

    except Exception as e:
        print(f"Error en grupo {id_grupo}: {str(e)}")
        with open(log_filename, 'a') as log_file:
            log_file.write("Error: " + str(e) + "\n")
    finally:
        if conexion:
            conexion.close()     

###

###PARA LOG
# VARIABLES PARA LOG GENERAL
ID_SERVICIO_GENE = 3
FCH_DATO_GENE = None
FCH_INICIO_LECT_GENE = None
FCH_FIN_LECT_GENE = None
CANT_LECT_GENE = None
OK_LECT_GENE = None
FAIL_LECT_GENE = None
COMENT_LECT_GENE = None
# VARIABLES PARA LOG GENERAL

### Métodos para Logs.
# INSERTAR DATOS EN LOG GENERAL
def insertar_datos_log(_conexion):  
    cursor = _conexion.cursor()
    
    FCH_DATO_GENE = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    
    cursor.callproc("fn_log_insertar_datos", [
        ID_SERVICIO_GENE,
        FCH_DATO_GENE
    ])
    
    _conexion.commit()
    id_log = cursor.fetchone()
    id_log = id_log[0]
    
    return id_log  
# INSERTAR DATOS EN LOG GENERAL

def insertar_datos_log_caf(_conexion):  
    cursor = _conexion.cursor()
    
    FCH_DATO_GENE = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    
    ID_SERVICIO_GENE = 4
    cursor.callproc("fn_log_insertar_datos", [
        ID_SERVICIO_GENE,
        FCH_DATO_GENE
    ])
    
    _conexion.commit()
    id_log = cursor.fetchone()
    id_log = id_log[0]
    
    return id_log  

# Actualizar datos del log en base de datos
def actualizar_log(_conexion, _id_log, _fch_inicio_lect_log, _fch_fin_lect_log, _cant_lect_log, _ok_lect_log, _fail_lect_log, _coment_lect_log):
    cursor = _conexion.cursor()
    cursor.callproc("fn_log_actualizar_datos", [
        _id_log,
        _fch_inicio_lect_log,
        _fch_fin_lect_log,
        _cant_lect_log,
        _ok_lect_log,
        _fail_lect_log,
        _coment_lect_log
    ])
    _conexion.commit()
    id_log = cursor.fetchone()
    id_log = id_log[0]

def insertar_datos_log_detalle_defontana(_conexion, _id_log, _status, _fch_dato_det, _fch_inicio_lect_det, 
                                         _fch_fin_lect_det, _cant_read, _cant_insert, _id_lectura, _cant_reintentos):
    
    cursor = _conexion.cursor()
    cursor.callproc("fn_log_detalle_insertar_datos_defontana", [
        _id_log,
        _status,
        _fch_dato_det,
        _fch_inicio_lect_det,
        _fch_fin_lect_det,
        _cant_read,
        _cant_insert,
        _id_lectura,
        _cant_reintentos
    ])
    _conexion.commit()
    id_log = cursor.fetchone()
    id_log = id_log[0]
# Fin insertar en log detalle de base de datos


# Diccionario global para rastrear hilos activos por grupo
active_threads = {} 

def main():
    conexion = connection_database()
    if not conexion:
        sys.exit(1)
    
    try:
        # Obtener los grupos iniciales
        lecturas_por_grupo = obtener_lecturas_por_grupo(conexion)
    except Exception as e:
        print(f"Error obteniendo grupos: {str(e)}")
        conexion.close()
        sys.exit(1)
    
    conexion.close()

    # Crear un hilo para cada grupo
    for id_grupo in lecturas_por_grupo.keys():
        # Verificar si ya hay un hilo activo para este grupo
        if id_grupo in active_threads and active_threads[id_grupo].is_alive():
            print(f"El hilo para el grupo {id_grupo} aún está en ejecución. Saltando...")
            continue

        t = Thread(target=manejar_grupo, args=(id_grupo,))
        print(f"Lanzando hilo para el grupo {id_grupo}")
        active_threads[id_grupo] = t
        t.start()

if __name__ == "__main__":
    main()