import sys
import time
import os

from datetime import datetime, timedelta

# VARIABLES PARA LOG GENERAL
ID_SERVICIO_GENE = 3 #3 ES DEFONTANA

nombre_archivo = os.path.abspath(sys.argv[0])# Obtener el nombre del archivo actual (incluyendo la ruta completa)
nombre_archivo_sin_extension = os.path.splitext(os.path.basename(nombre_archivo))[0]
nombre_log= 'log_sandbox_'+nombre_archivo_sin_extension+'.txt'

ruta_archivo = os.path.join("C:\\Program Files\\Oenergy\\APIServiceDefontana", nombre_log)


def insertar_datos_log(_conexion, called_execution_time):  
#def insertar_datos_log(_conexion):  
    cursor = _conexion.cursor()
    
    #FCH_DATO_GENE = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    # Añadir la fecha actual al tiempo
    FCH_DATO_GENE = datetime.now().strftime('%Y-%m-%d ') + called_execution_time



    cursor.callproc("fn_log_insertar_datos", [
        ID_SERVICIO_GENE,
        FCH_DATO_GENE
    ])
    
    _conexion.commit()
    id_log = cursor.fetchone()
    id_log = id_log[0]
    
    return id_log  
# INSERTAR DATOS EN LOG GENERAL


def main ():
    print ("Hola soy una sandbox")
    ID_LOG = insertar_datos_log(ID_SERVICIO_GENE,)
    print ("Id log:" + str(ID_LOG))

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
        #procesar_grupo_obtener_lecturas(id_grupo)
        #envio la hora de ejecucion para que sea todo más cool
        #para ver en la hora que fue llamada una ejecución.-
        called_execution_time = hora_ejecucion.strftime('%H:%M:%S')
        procesar_grupo_obtener_lecturas(id_grupo,called_execution_time)
        print(f"Ejecutado proceso para el grupo {id_grupo} a las {hora_ejecucion.strftime('%H:%M:%S')}")

def procesar_grupo_obtener_lecturas(id_grupo,called_execution_time):
    log_filename = os.path.join(log_directory, f"log_grupo_lectura_{id_grupo}.txt")
    '''ahora = datetime.now()
    cadena_fecha = ahora.strftime("%H:%M:%S.%f %d/%m/%Y")
    '''
    print ("Tiempo de orquestación.")
    print("called exec time: ", str(called_execution_time))
    try:
        conexion = connection_database()

        if conexion:
            
            #print ("ID LOG "+str(ID_LOG))
            #ID_LOG = 999
            #revisar en bd para que no arroje -1 never.
            
            lecturas_grupo_actual = obtener_lecturas_grupo_actual(conexion, id_grupo)

            # escribo en el log cada lectura que haré secuencial.
            escribir_archivo_log(id_grupo, lecturas_grupo_actual)


            FCH_LOG= datetime.now().strftime('%Y-%m-%d ') + called_execution_time
            #print("fch_log:" + str(FCH_LOG))

            #ID_LOG = insertar_datos_log_caf(conexion, called_execution_time)
            ID_LOG = insertar_datos_log(conexion, called_execution_time)
            
            print ("ID LOG: "+str(ID_LOG))
           
            #id logsuelo
            #testyeoeoeoeo



            # Proceso cada lectura y evaluo QUE ES
            for lectura in lecturas_grupo_actual:                
                procesar_lectura(lectura, log_filename, ID_LOG, called_execution_time)

                #procesar_lectura(lectura, log_filename,called_execution_time)

                #procesar_lectura(lectura, log_filename)#, ID_LOG)

    except Exception as e:
        print(f"Error en grupo {id_grupo}: {str(e)}")
        with open(log_filename, 'a') as log_file:
            log_file.write("Error: " + str(e) + "\n")
    finally:
        if conexion:
            conexion.close()     

def procesar_lectura(lectura, log_filename, ID_LOG, called_execution_time):
#def procesar_lectura(lectura, ruta_archivo,ID_LOG):    
#def procesar_lectura(lectura, ruta_archivo, called_execution_time):
#def procesar_lectura(lectura, ruta_archivo):

    #conexion = connection_database()

    tipo_lectura = lectura['endpoint']  # Asumiendo que 'endpoint' contiene la información de tipo de factura

    # Obtener el día actual y la hora actual
    hoy = datetime.now()
    dia_actual = hoy.day
    hora_actual = hoy.time()

    #para validar si la lectura debe correr hoy
    intervalo_inicio_numero_dia_on = lectura['intervalo_inicio_numero_dia_on']
    intervalo_fin_numero_dia_on = lectura['intervalo_fin_numero_dia_on']
    #para validar si la lectura debe correr en el rango de hora actual
    intervalo_inicio_hora_on = lectura['intervalo_inicio_hora_on']
    intervalo_fin_hora_on = lectura['intervalo_fin_hora_on']

    # Comprobar si el día actual está dentro del rango permitido
    if intervalo_inicio_numero_dia_on <= dia_actual <= intervalo_fin_numero_dia_on:
        # Comprobar si la hora actual está dentro del rango permitido
        if intervalo_inicio_hora_on <= hora_actual <= intervalo_fin_hora_on:
       
            if tipo_lectura == "facturas_ventas":     
                #procesar_factura_venta(lectura, ruta_archivo, ID_LOG)#print ()
                procesar_factura_venta(lectura, ruta_archivo, ID_LOG, called_execution_time)
            
            elif tipo_lectura == "get_caf":             
                #procesar_caf(lectura, ruta_archivo, ID_LOG)
                procesar_caf(lectura, ruta_archivo, ID_LOG, called_execution_time)
            
            else:   
                print(f"Tipo de factura desconocido, id_lectura:  {lectura['id_lectura']}")

                with open(ruta_archivo, "a") as archivo:                   
                    archivo.write( f"Tipo de lectura desconocido, id_lectura:  {lectura['id_lectura']}")
        
        else:
            print(f"Fuera del horario permitido, id_lectura:  {lectura['id_lectura']}")

            with open(ruta_archivo, "a") as archivo:  
                archivo.write(str(datetime.now())+f" | Fuera del horario permitido, id_lectura:  {lectura['id_lectura']}")


    else:
        print(f"Fuera de los días permitidos, id_lectura:  {lectura['id_lectura']}")

        with open(ruta_archivo, "a") as archivo:  
            archivo.write(str(datetime.now())+f" | Fuera de los días permitidos, id_lectura:  {lectura['id_lectura']}")
