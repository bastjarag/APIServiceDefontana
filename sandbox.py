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




def procesar_factura_compra(lectura, ruta_archivo, ID_LOG, called_execution_time):
#def procesar_factura_compra(lectura, ruta_archivo, ID_LOG):   
#def procesar_factura_compra(lectura, ruta_archivo, ID_LOG):    
#def procesar_factura_compra (lectura, ruta_archivo):

    #la lectura es la parametrización desde la tabla
    tipo_factura = lectura['endpoint']  #
    id_lectura = lectura['id_lectura']
    id_emp = lectura['id_emp']
    url_sii = lectura['url']
    user_sii = lectura['user_sii']
    pw_sii = lectura['pw_sii']
    max_reintentos = lectura['max_reintentos']
    token_sii = lectura['token']
    id_gpo = lectura['id_gpo']

    resta_mes_periodo = lectura['resta_mes_periodo']



    print ("ID GPO: "+str(id_gpo))

    log_filename = os.path.join(log_directory, f"log_grupo_lectura_{id_gpo}.txt")
    '''ahora = datetime.now()
    cadena_fecha = ahora.strftime("%H:%M:%S.%f %d/%m/%Y")'''

    ruta_archivo = log_filename

    '''print ("Id_lectura" +str(id_lectura))
    print ("ruta archivo:" +str(ruta_archivo))'''

    #print ("Entré a procesar Factura de Compra")
   
    cone = connection_database() 
    if cone is None:
        print("ERROR BD")
        with open(ruta_archivo, "a") as archivo:                   
            archivo.write( "\n***** ERROR CONEXIÓN BD!!!!: "+str(datetime.now())+" de COMPRAS SII:")
    else:
        
        try:
            #print ("Comienzo a procesar factura compra..")
            OK_LECT_GENE_compras = 0
            FAIL_LECT_GENE_compras = 0

            ID_LECTURA_DETA_compras = id_lectura

            ID_ESTATUS_DETA_compras = INICIO_OK
            #deprecado.
            #ID_LOG_DETA_compras = insertar_datos_log(cone) # Insertar datos en tbl_log y devolver su id
            ID_LOG_DETA_compras = ID_LOG



            MENSAJE_COMPLETO_GEN_compras = 'Ok'
            
            #FCH_INICIO_LECT_GENE_compras = datetime.now().strftime("%d-%m-%Y %H:%M:%S") # fch_dato en tbl_log
            FCH_INICIO_LECT_GENE = datetime.now().strftime('%Y-%m-%d ') + called_execution_time# fch_dato en tbl_log
            agregar_auth_cache_compra = False  # Inicialmente, no agregamos auth_cache en COMPRAS.-

            '''print("Empresa a Leer procesar_factura_compra:")
            print ("Empresa:"+ str(id_emp) )  
            print ("Rut SII:"+ user_sii)'''

            ID_ESTATUS_DETA_compras = ENDPOINTS_OK #LECTURAS DE METODOS A LEER OBTENIDAS


            header = payload_header(token_sii)#metodo payload_header con token                         
            body = payload_body(user_sii, pw_sii)# Llamar al método payload_body con los valores obtenidos   

            intento = 0
            for intento in range(max_reintentos):
                print ("Intento: "+str(intento))

                #guardo el reintento que me tiró datos
                CANT_REINTENTOS_DETA_compras = intento #intento+1

                #FCH_INICIO_LECT_DETA_compras = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 
                FCH_INICIO_LECT_DETA_compras = datetime.now().strftime("%d-%m-%Y ") + called_execution_time

                if agregar_auth_cache_compra:
                    if "&auth_cache=0" not in url_sii:
                        url_sii += "&auth_cache=0"  # Agregar auth_cache si no está presente
                else:
                    url_sii = url_sii.replace("&auth_cache=0", "")  # Quitar auth_cache si está presente    

                #aqui se debe pasar la resta_mes_periodo
                #respuesta_completa = obtener_respuesta_compras(user_sii, header, body, url_sii)
                respuesta_completa = obtener_respuesta_compras(user_sii, header, body, url_sii,resta_mes_periodo)

                
                
                respuesta_json = respuesta_completa.json()

                if respuesta_completa.status_code == 200:        
                    break  # Salir del bucle de reintentos si la respuesta es exitosa

                if respuesta_completa.status_code == 400: 
                    # Agregar auth cache a url, si viene con el error feo de parte de SII.
                    agregar_auth_cache_compra = True
                    continue

                if agregar_auth_cache_compra and respuesta_completa.status_code == 500 :                          
                    # Agregar auth cache a url, si viene con el error feo de parte de SII.
                    
                    mensaje = respuesta_json.get('message') #seguro es error.#  obtener el valor de "message"

                    print ("Error en respuesta, Status code: "+str(respuesta_completa.status_code))
                    print ("Mensaje error:"+mensaje,"\n")

                    with open(ruta_archivo, "a") as archivo:                   
                        archivo.write("Error en respuesta, Status code: "+str(respuesta_completa.status_code))
                        archivo.write("Mensaje error:"+mensaje+"\n")

                    agregar_auth_cache_compra = False 
                        
                    continue

                if intento < max_reintentos - 1:
                    #intento la cantidad max de reintentos     
                    with open(ruta_archivo, "a") as archivo: 
                        archivo.write("\nLectura fallida intento numero: "+str(intento+1)+" | Url: "+url_sii
                                        + "Usuario SII:" + user_sii)

                    tiempo_espera = 10  # esperar 10 segundos  # Esperar antes del próximo reintento
                    time.sleep(tiempo_espera)
            #fin bucle intento (de intento a max reintentos)       


            #headers de respuesta de la solicitud:
            limit = respuesta_completa.headers.get('X-RateLimit-Limit')
            remaining = respuesta_completa.headers.get('X-RateLimit-Remaining')

            print ("intentos restantes dia:",str(remaining))

            ''' with open(ruta_archivo, "a") as archivo:                   
                archivo.write("Intentos restantes dia:",str(remaining))'''

            started_at = respuesta_completa.headers.get('X-Stats-StartedAt')
            ended_at = respuesta_completa.headers.get('X-Stats-EndedAt')
            _cod_rpta = respuesta_completa.status_code



            if respuesta_completa.status_code == 200:  

                MENSAJE_COMPLETO_GEN_compras +=  'OK' + "Rut: "+user_sii + " Id lectura:" + str(ID_LECTURA_DETA_compras) + "\n"  


                print ("*Código Respuesta 200*")   
                data = respuesta_json["data"]# sólo me interesa lo que está dentro de data, ya que luego de 'data' del json hay información de impresion..
                
                #Listado para insertar en tabla tbl_clientes_proveedores
                keys = lista_rut_razon_soc(data)# Save the keys from the data to a list. rut, dv, razon social.
                cant_raz_sociales = len(keys)

                print ("- Total Filas en Data del endpoint: ",cant_raz_sociales)                    
                filas_afectadas = insert_clients_and_providers(keys, cone)#Insert the clients and providers in the `tbl_clientes_proveedores` table.
                print ("- Filas insertadas en tbl_clientes_proveedores: ",filas_afectadas)  
                
                if filas_afectadas > 1 :
                    print("Nuevos Clientes proveedores: "+str(filas_afectadas))
                    with open(ruta_archivo, "a") as archivo:                   
                        archivo.write( "Nuevos Clientes proveedores: "+str(filas_afectadas) )     

                #fin inserción listado en tabla tbl_clientes_proveedores.

                ##insertar en tabla tbl_clientes_proveedores_empresas
                #tabla rompimiento entre tbl_clientes_proveedores y empresas:
                lista_tbl_clientes_proveedores_empresas = lista_para_tbl_clientes_proveedores_empresas_compras(data, id_emp, cone)
                
                filas_insertadas_tbl_cli_prov_emp = insert_lista_para_tbl_clientes_proveedores_empresas(lista_tbl_clientes_proveedores_empresas, cone)
                
                if filas_insertadas_tbl_cli_prov_emp > 1 :
                    print("Nuevos Proveedor en tbl_clientes_proveedores_empresas: "+str(filas_insertadas_tbl_cli_prov_emp))
                    with open(ruta_archivo, "a") as archivo:                   
                        archivo.write( "Nuevo Proveedor en tbl_clientes_proveedores_empresas: "+str(filas_insertadas_tbl_cli_prov_emp) )                      

                #print ("inserción en tbl_clientes_proveedores_empresas: "+str(filas_insertadas_tbl_cli_prov_emp))

                df = crear_dataframe_compras(id_emp, data, cone)#Para crear mi Data frame, paso la id empresa de la que estoy leyendo.                        
                
                #prueba de datos del df, deja un csv en el escritorio.
                #df.to_csv('~/Desktop/datos_sii_compras.csv', index=False)    

                json_nuevo = crear_json_con_df(df) #transformo el df a json
                json_nuevo = json.loads(json_nuevo)

                compras_insertadas = obtener_valores_compras(json_nuevo, cone)
                print ("- Compras insertadas: ",compras_insertadas)   
                print ("**************")    

                #escribo en log txt del escritorio.
                with open(ruta_archivo, "a") as archivo:                   
                    archivo.write("\n{}  Rut: {} | Compras Insertadas en BD: {} | Filas Rpta. API: {} | Cantidad intentos restantes: {} | LECTURA: {} | \n"
                        .format( started_at, user_sii, compras_insertadas,cant_raz_sociales , remaining, _cod_rpta))
            
                #INSERCION EN TABLA ANTIGUA
                ##log en bd:
                #insertar_tbl_log_lecturas_sii(_cod_rpta,compras_insertadas, remaining,started_at,ended_at,usuario_actual,nombre_de_este_archivo,mensaje,cone) 
            
                CANT_READ_DETA_compras = cant_raz_sociales
                CANT_INSERT_DETA_compras = compras_insertadas
                FCH_DATO_DETA_compras = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 
                FCH_FIN_LECT_DETA_compras = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                #ID_ESTATUS_DETA = RESPUESTA_OK   
                
                ID_ESTATUS_DETA_compras =  FIN_OK              
                
                
                #INSERCIÓN EN LOG DE DETALLE:
                insertar_datos_log_detalle_sii(cone, ID_LOG_DETA_compras, ID_ESTATUS_DETA_compras, FCH_DATO_DETA_compras, FCH_INICIO_LECT_DETA_compras, FCH_FIN_LECT_DETA_compras, CANT_READ_DETA_compras, CANT_INSERT_DETA_compras, ID_LECTURA_DETA_compras,CANT_REINTENTOS_DETA_compras)
                
                OK_LECT_GENE_compras += 1

            else: 
                
                #  obtener el valor de "message"
                mensaje = respuesta_json.get('message') #seguro es error.

                # Ahora, "mensaje" contendrá el valor de "message"
                print("Mensaje de respuesta error:", mensaje)

                MENSAJE_COMPLETO_GEN_compras += MENSAJE_COMPLETO_GEN_compras + "Rut: "+user_sii + " Id lectura:" + str(ID_LECTURA_DETA_compras) + " Mensaje error: " + mensaje + "\n"

                # Fecha actual
                fecha_actual_str = datetime.now().strftime("%d-%m-%Y %H:%M:%S")    

                compras_insertadas_compras = 0
                filas_afectadas_compras = 0
                _cod_rpta = respuesta_completa.status_code #será distinto a 200
                print ("No hubo status 200 despues de N intentos con tiempo de espera.")
                with open(ruta_archivo, "a") as archivo:
                    mensaje = respuesta_completa.text
                    archivo.write("{} | LECTURA: {} | Cantidad intentos restantes: {} | Filas Afectadas: {} | Mensaje : {}\n"
                                .format(fecha_actual_str, _cod_rpta, remaining, filas_afectadas ,mensaje))             # Manejar el caso de falla después de los reintentos 

                    archivo.write("\n"+"Intento: numero"+ str(CANT_REINTENTOS_DETA_compras) + "Codigo Rpta:" + 
                                    str(_cod_rpta) + "Intentos restantes:"+str(remaining)+ "Mensaje"+"\n"
                                    + mensaje )


                FCH_DATO_DETA_compras = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 
                FCH_FIN_LECT_DETA_compras = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 
                CANT_READ_DETA_compras = 0
                CANT_INSERT_DETA_compras = 0
                
                
                ID_ESTATUS_DETA_compras = RESPUESTA_FAIL #al menos una respuesta entró en error.

                #INSERCIÓN BD tabla tbl_log_detalle_sii
                insertar_datos_log_detalle_sii(cone, ID_LOG_DETA_compras, ID_ESTATUS_DETA_compras, FCH_DATO_DETA_compras, FCH_INICIO_LECT_DETA_compras, FCH_FIN_LECT_DETA_compras, CANT_READ_DETA_compras, CANT_INSERT_DETA_compras, ID_LECTURA_DETA_compras, CANT_REINTENTOS_DETA_compras)
                
                FAIL_LECT_GENE_compras += 1
            
            #aca (?)

            # Fecha actual
            fecha_actual_str = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

            with open(ruta_archivo, "a") as archivo:                   
                archivo.write( "\n***** FIN Lecturas: "+str(fecha_actual_str)+" de COMPRAS SII ***** ")                         
            cone.commit()
            #cone.close() 
            print() 

            COMENT_LECT_GENE_compras = MENSAJE_COMPLETO_GEN_compras

            if FAIL_LECT_GENE_compras == CANT_LECT_GENE_compras:
                COMENT_LECT_GENE_compras = "API CON FALLA TOTAL"
            #else:
            #    COMENT_LECT_GENE = "TERMINADO OK"
            
            FCH_FIN_LECT_GENE_compras = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 

            # actualizar datos de tbl_log
            actualizar_log(cone, ID_LOG_DETA_compras, FCH_INICIO_LECT_GENE_compras, FCH_FIN_LECT_GENE_compras, CANT_LECT_GENE_compras, OK_LECT_GENE_compras, FAIL_LECT_GENE_compras, COMENT_LECT_GENE_compras)
            
            cone.close()

        #FIN TRY.

        except Exception as e:
            import traceback
            error_msg = traceback.format_exc()
            print("Error:\n", error_msg)

        
        
        '''with open(ruta_archivo, "a") as archivo:                   
            archivo.write("Inicio procesar factura compra")'''
