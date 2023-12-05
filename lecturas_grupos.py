import os
import sys
import time
import psycopg2
from threading import Thread
import json
import pandas as pd
import requests
from datetime import datetime, timedelta

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from base64 import b64decode

import math

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

# VARIABLES PARA LOG GENERAL
ID_SERVICIO_GENE = 3 #3 ES DEFONTANA
FCH_DATO_GENE = None
FCH_INICIO_LECT_GENE = None
FCH_FIN_LECT_GENE = None
CANT_LECT_GENE = None
OK_LECT_GENE = None
FAIL_LECT_GENE = None
COMENT_LECT_GENE = None
# VARIABLES PARA LOG GENERAL

# VARIABLES PARA LOG DETALLE
ID_LOG_DETA = None
FCH_DATO_DETA = None
FCH_INICIO_LECT_DETA = None
FCH_FIN_LECT_DETA = None
CANT_READ_DETA = None
CANT_INSERT_DETA = None
ID_LECTURA_DETA = None
ID_ESTATUS_DETA = None
# VARIABLES PARA LOG DETALLE 

### new code
nombre_archivo = os.path.abspath(sys.argv[0])# Obtener el nombre del archivo actual (incluyendo la ruta completa)
nombre_archivo_sin_extension = os.path.splitext(os.path.basename(nombre_archivo))[0]
nombre_log= 'log_defontana_'+nombre_archivo_sin_extension+'.txt'
ruta_archivo = os.path.join("C:\\Program Files\\Oenergy\\APIServiceDefontana", nombre_log)

# Verificar si el archivo existe, y si no, crearlo
if not os.path.isfile(ruta_archivo):
    with open(ruta_archivo, 'w'):
        pass 

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
            archivo.write("FECHA: {} || Archivo: {} || Error de conexion BD: {}\n".format(fecha_actual, nombre_log, str(e)))#nombre_de_este_archivo, str(e)))

#método para calcular la cantidad de páginas que se deben recorrer en obtener_respuesta       
def calcular_total_paginas(total_items, items_por_pagina):
    return math.ceil(total_items / items_por_pagina)

##### Para Facturas de venta
#llamada de facturas de ventas
#def obtener_respuesta(_url, _item_por_pagina, _numero_pagina, _token):

def old_fashionedobtener_respuesta_old_fashioned(_url, _item_por_pagina, _numero_pagina, _token,resta_mes_periodo):

    # Obtener la fecha actual en formato de cadena "YYYY-MM-DD"
    fecha_actual_str = datetime.now().strftime("%Y-%m-%d")

    # Convertir la cadena de fecha en un objeto datetime
    fecha_actual = datetime.strptime(fecha_actual_str, "%Y-%m-%d")
    #print(fecha_actual)
    
    #### test1
    #fecha_actual_str = "2023-11-17"
    #fecha_actual_str = "2023-11-16"

    # Calcular la fecha de 5 días atrás
    #fecha_5_dias_atras = fecha_actual - timedelta(days=5)

    #codigo deprecado..
    '''#Fecha 5 días atras
    fecha_5_dias_atras = fecha_actual - timedelta(days=30)
    # Convertir la fecha de 5 días atrás en formato de cadena "YYYY-MM-DD"
    fecha_5_dias_atras_str = fecha_5_dias_atras.strftime("%Y-%m-%d")
    # reemplazar or lectura máxima 5 días atras....
    #fecha_5_dias_atras_str = "2023-08-01"
    #fecha_5_dias_atras_str = "2023-09-01"'''
    #fin codigo deprecado, para que sea desde principio de mes en ves de solo 5 dias atras.


    # Obtener el primer día del mes actual
    fecha_principio_mes = fecha_actual.replace(day=1)

    # Convertir la fecha del primer día del mes en formato de cadena "YYYY-MM-DD"
    fecha_principio_mes_str = fecha_principio_mes.strftime("%Y-%m-%d")


    # Luego, convierte las fechas a cadenas antes de usarlas en la URL
    #url = _url.replace(":fch_inicio", fecha_5_dias_atras_str).replace(":fch_fin", fecha_actual_str).replace(":item_por_pagina", _item_por_pagina).replace(":numero_pagina", _numero_pagina)
    url = _url.replace(":fch_inicio", fecha_principio_mes_str).replace(":fch_fin", fecha_actual_str).replace(":item_por_pagina", _item_por_pagina).replace(":numero_pagina", _numero_pagina)
         
    #print (url)

    payload = {}

    headers = {
    'Authorization': 'Bearer {}'.format(_token)
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    #print("respuesta completa")
    #print(response.json())
    return response

#parchado para que funcione como mes entero anterior, considerando diciembre y enero.
def obtener_respuesta(_url, _item_por_pagina, _numero_pagina, _token, resta_mes_periodo):
    fecha_actual = datetime.now()
    
    #Fecha de Prueba 15/01/2025
    #fecha_actual = datetime(2024, 1, 15)
    #print("Fecha actual (de prueba):", fecha_actual) 

    #print ("fch actual: " +str(fecha_actual))
    #print ("resta mes periodo: "+str(resta_mes_periodo))

    #hardcodeo para probrar que me lea un mes anterior.
    #resta_mes_periodo = 1 

    if resta_mes_periodo == 0:
        # Uso de fechas actuales
        fecha_inicio = fecha_actual.replace(day=1)
        fecha_fin = fecha_actual
    else:        
        #print("Resta mes periodo: "+str(resta_mes_periodo))

        # Calculando el primer día del mes anterior y el último día de ese mes
        primer_dia_mes_anterior = fecha_actual.replace(day=1) - timedelta(days=1)
        primer_dia_mes_anterior = primer_dia_mes_anterior.replace(day=1, month=primer_dia_mes_anterior.month - resta_mes_periodo + 1)
        
        # Ajustar año y mes para el último día del mes anterior
        mes_siguiente = primer_dia_mes_anterior.month + 1
        anio_siguiente = primer_dia_mes_anterior.year
        
        if mes_siguiente > 12: #En el caso que estemos en Diciembre.
            mes_siguiente = 1
            anio_siguiente += 1

        ultimo_dia_mes_anterior = datetime(anio_siguiente, mes_siguiente, 1) - timedelta(days=1)

        fecha_inicio = primer_dia_mes_anterior
        fecha_fin = ultimo_dia_mes_anterior

        #print("fecha_inicio: " + str(fecha_inicio) + "| fecha_fin: " + str(fecha_fin) )

    # Convertir las fechas a formato de cadena "YYYY-MM-DD"
    fecha_inicio_str = fecha_inicio.strftime("%Y-%m-%d")
    fecha_fin_str = fecha_fin.strftime("%Y-%m-%d")

    # Construir la URL con las fechas correspondientes
    url = _url.replace(":fch_inicio", fecha_inicio_str).replace(":fch_fin", fecha_fin_str).replace(":item_por_pagina", _item_por_pagina).replace(":numero_pagina", _numero_pagina)

    # Resto del código para realizar la solicitud HTTP...
    payload = {}
    headers = {
        'Authorization': 'Bearer {}'.format(_token)
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    
    return response



def insertar_saleList(_respuesta, _conexion, _id_emp, _token):
    saleLists = _respuesta['saleList']

    filas_afectadas_total = 0 #facturas insertadas.
    for saleList in saleLists:
        
        documentType = saleList['documentType']

        # Si documentType no es 'FVAELECT', continuar con el siguiente registro
        if documentType != 'FVAELECT':
            print("Documento NO es factura electrónica (FVAELECT)!!!!!!!")
            print("Es: "+str(documentType))
            continue

        #print(documentType)
        firstFolio = saleList['firstFolio']
        lastFolio = saleList['lastFolio']
        status = saleList['status']
        emissionDate = saleList['emissionDate']
        dateTime = saleList['dateTime']
        expirationDate = saleList['expirationDate']
        
        # rut a buscar el tbl_clientes_proveedor
        clientFile = saleList['clientFile']         
        contactIndex = saleList['contactIndex']
        paymentCondition = saleList['paymentCondition']
        sellerFileId = saleList['sellerFileId']
        billingCoin = saleList['billingCoin']
        billingRate = saleList['billingRate']
        shopId = saleList['shopId']
        priceList = saleList['priceList']
        giro = saleList['giro']
        city = saleList['city']
        district = saleList['district']
        contact = saleList['contact']
        
        # Datos dentro de attachedDocuments en JSON
        attachedDocumentsDate = None
        attachedDocumentType = None
        attachedDocumentName = None
        attachedDocumentNumber = None
        attachedDocumentTotal = None
        attachedDocumentTotalDocumentTypeId = None
        attachedDocumentFolio = None
        attachedDocumentsReason = None
        attachedDocumentsGloss = None
        attachedDocuments = saleList['attachedDocuments']

        for attachedDocument in attachedDocuments:
            #print(attachedDocumentsDate)
            attachedDocumentsDate = attachedDocument['date']
            attachedDocumentType = attachedDocument['attachedDocumentType']
            attachedDocumentName = attachedDocument['attachedDocumentName']
            attachedDocumentNumber = attachedDocument['attachedDocumentNumber']
            attachedDocumentTotal = attachedDocument['attachedDocumentTotal']
            attachedDocumentTotalDocumentTypeId = attachedDocument['documentTypeId']
            attachedDocumentFolio = attachedDocument['folio']
            attachedDocumentsReason = attachedDocument['reason']
            attachedDocumentsGloss = attachedDocument['gloss']
        
        gloss = saleList['gloss']
        affectableTotal = saleList['affectableTotal']
        exemptTotal = saleList['exemptTotal']
        taxeCode = saleList['taxeCode']
        taxeValue = saleList['taxeValue']
        
        # Datos de documentTaxes en JSON       
        # #ender no quiere guardar esto.. 27/07 
        #documentTaxes = 'null'
        #documentTaxes = saleList['documentTaxes'] # json
        #documentTaxes = json.dumps(documentTaxes)        
      
        ventaRecDesGlobal = saleList['ventaRecDesGlobal']
        total = saleList['total']
        
        # Datos dentro de voucherInfo en JSON
        voucherInfoFolio = None
        voucherInfoType = None
        voucherInfo = saleList['voucherInfo'] # json
        for  voucherinfos in voucherInfo:
            voucherInfoFolio = voucherinfos['folio'] # PK de tbl_datos_defontana_ventas_v2
            voucherInfoType = voucherinfos['type']
            
            '''print ("folio" ,voucherInfoFolio)
            print("token", _token)'''       
            
        
        isTransferDocument = saleList['isTransferDocument']
        if isTransferDocument != 'N':
            print ("Es documento de traspaso!!!!!!!!!")
            continue


        timestamp = saleList['timestamp']        
        
        # buscar rut cliente en tbl_clientes_proveedores
        rut_sin_dv = clientFile[:-1].replace(".","").replace("-","") # quitar . - dv
        dv = clientFile[-1]
        id_cliente = None
        
        #buscar e insertar cliente proveedor en la tabla d ela bd...
        try:
            cursor = _conexion.cursor()
            #cursor.callproc('fnc_defontana_bus_ins_cliprov', [
            cursor.callproc('fn_defontana_buscar_insertar_cliprov', [   
                                                    clientFile,
                                                    rut_sin_dv,
                                                    dv
                                                    ])
            id_cliente = cursor.fetchone()
            id_cliente = id_cliente[0] # Obtener el id cliente
            #print("id_cli",id_cliente)
            
            # print(id_cliente)
            #_conexion.commit()   
            # Incrementar el valor de filas_afectadas_total por el valor de retorno de la función.
            #filas_afectadas_total += id_cliente
            if id_cliente == 0: 
                _conexion.rollback()
            #_conexion.commit()   
           
        except Exception as e:
            _conexion.rollback()
            print("ERROR: {}".format(e))
            with open (ruta_archivo, 'a') as archivo:
                archivo.write("\nError al obtener ID de Cliente Proveedor desde la base de datos...")
            
        
        #print("antes de insertar en facturas de venta.s...")   
        # Insertar datos en tbl_datos_defontana_ventas
        if id_cliente > 0:
            try:
            # print("dentro de factrura vendta....")
                fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cursor = _conexion.cursor()
                #cursor.callproc('fnc_defontana_ins_ventas_b', [

                cursor.callproc('fn_defontana_insertar_factura_venta', [    
                                                        voucherInfoFolio,
                                                        _id_emp,
                                                        id_cliente,
                                                        fecha_actual,
                                                        documentType,
                                                        firstFolio,
                                                        lastFolio,
                                                        status,
                                                        emissionDate,
                                                        dateTime,
                                                        expirationDate,
                                                        contactIndex,
                                                        paymentCondition,
                                                        sellerFileId,
                                                        billingCoin,
                                                        billingRate,
                                                        shopId,
                                                        priceList,
                                                        giro,
                                                        city,
                                                        district,
                                                        contact,
                                                        attachedDocumentsDate,
                                                        attachedDocumentType,
                                                        attachedDocumentName,
                                                        attachedDocumentNumber,
                                                        attachedDocumentTotal,
                                                        attachedDocumentTotalDocumentTypeId,
                                                        attachedDocumentFolio,
                                                        attachedDocumentsReason,
                                                        attachedDocumentsGloss,
                                                        gloss,
                                                        affectableTotal,
                                                        exemptTotal,
                                                        taxeCode,
                                                        taxeValue,
                                                        #documentTaxes, #no quiere guardar el array de impuestos endeer
                                                        ventaRecDesGlobal,
                                                        total,
                                                        voucherInfoType,
                                                        #inventoryInfo,
                                                        #customFields,
                                                        #exportData,
                                                        isTransferDocument,
                                                        timestamp
                                                        ])
                
                resultado = cursor.fetchone()# Recupera el valor de retorno de la función.                
                valor_retorno = resultado[0]
                
                #probar que inserte facturas...
                print("*"*20)
                print("factura: ",voucherInfoFolio)
                print("insertó factura: ",valor_retorno)
                
                
                if valor_retorno == 0:
                    _conexion.rollback()

                if valor_retorno == 2: #factura ya existe en bd, sumar a la variable que me cuenta las facturas en bd.
                    f_e_bd = f_e_bd +1 
                        
                    
                #_conexion.commit()   #forzar la insercion
                              
            except Exception as e:
                _conexion.rollback()
                print("ERROR: {}".format(e))
                with open (ruta_archivo, 'a') as archivo:
                    archivo.write("\nError al insertar factura en la base de datos..")


            details = saleList['details'] #items de cada factura...

            valor_retorno_detalle = 0 #para retornar el valor de detalles insertados, consultado despues...
           
           
           
            # Insertar detalles de ventas
            if valor_retorno > 0: #si la factura se insertó, me insertará detalle...
                for detail in details: #recorro cada detalle..
                    detailLine = detail['detailLine']
                    type = detail['type']
                    code = detail['code']
                    count = detail['count']
                    price = detail['price']
                    isExempt = detail['isExempt']
                    discountType = detail['discountType']
                    discountValue = detail['discountValue']
                    comment = detail['comment']
                    total = detail['total']
                    priceList = detail['priceList']                    
                    analysis = detail['analysis']
                    accountNumber = detail['infAnalysis']['accountNumber']
                    businessCenter = detail['infAnalysis']['businessCenter']
                    classifier01 = detail['infAnalysis']['classifier01']
                    classifier02 = detail['infAnalysis']['classifier02']
                    
                    try:
                        
                        cursor = _conexion.cursor() #linea a lineas del detalle de facturas.-.
                       
                        cursor.callproc('fn_defontana_insertar_detalle_de_venta', [    
                                                                detailLine,
                                                                voucherInfoFolio, #ESTE ES EL FOLIO DE LA FACTURA!
                                                                _id_emp, #id de la empresa (gsd, oym, etc)
                                                                id_cliente, #id del cliente que está comprando 
                                                                type,
                                                                code,
                                                                count,
                                                                price,
                                                                isExempt,
                                                                discountType,
                                                                discountValue,
                                                                comment,
                                                                total,
                                                                priceList,
                                                                analysis,
                                                                accountNumber,
                                                                businessCenter,
                                                                classifier01,
                                                                classifier02
                                                                ])
                        
                        resultado_detalle = cursor.fetchone()
                        
                        valor_retorno_detalle = resultado_detalle[0]
                        #print(valor_retorno_detalle)
                        
                        if valor_retorno_detalle == 0: 
                            _conexion.rollback()
                        #_conexion.commit()                

                    except Exception as e:
                        _conexion.rollback()
                        print("ERROR: {}".format(e)) 
                        with open(ruta_archivo, 'a') as archivo:
                            archivo.write("Error al Insertar facturas en la base de datos...") 
                print("insertó detalle: ", valor_retorno_detalle)    
                if valor_retorno_detalle > 0: #se insertó el detalle, insertaré ahora los b64...
                    #ahora voy a insertar b64 de pdf y xml...

                    #insertar pdf por folio, y empresa:
                    #testeo para el 14-11.-
                    '''try:
                        #testeo.
                        #print("id emp",_id_emp)
                        #print("folio", voucherInfoFolio)
                        b64 = obtener_b64_por_folio(_token,voucherInfoFolio)
                        #print("*"*20)
                        rpta_pdf = insertar_pdf(_id_emp, voucherInfoFolio, b64, _conexion)
                        print("Insertó pdf:",rpta_pdf)
                        if rpta_pdf < 0 : #retorna -1 en caso de error...
                            _conexion.rollback()
                    except Exception as e:
                        _conexion.rollback()
                        print("Fallo la insercion de factura.") '''     

                    try:
          
                        b64 = obtener_b64_por_folio(_token, voucherInfoFolio)                        

                        if b64 is not None:
                            rpta_pdf = insertar_pdf(_id_emp, voucherInfoFolio, b64, _conexion)
                            print("Insertó pdf:", rpta_pdf)
                            if rpta_pdf < 0:  # retorna -1 en caso de error...
                                _conexion.rollback()
                        else:
                            #revisar rollback cuando está vacío el pdf
                            _conexion.rollback()
                            print("No se encontró el documento PDF o está vacío para el folio:", voucherInfoFolio)
                            # Aquí puedes manejar el caso de que b64 sea None como consideres necesario
                    except Exception as e:
                        _conexion.rollback()
                        print("Fallo la inserción de factura:", e)



                    #insertar xml por folio y empresa...

                    #deprecado:

                    '''try:
                        if rpta_pdf > 0:
                            xml_b64 = obtener_xml_b64_por_folio(_token,voucherInfoFolio)   
                            rpta_xml = insertar_xml(_id_emp, voucherInfoFolio, xml_b64, _conexion)
                            print ("Insertó xml:", rpta_xml)
                            filas_afectadas_total +=  rpta_xml #AQUI ES CUANDO INSERTO EL REGISTRO ENTERO

                            if rpta_xml < 0 : #retorna -1 en caso de error...
                                _conexion.rollback()
                    except Exception as e:
                        _conexion.rollback()
                        print("Fallo la insercion de xml de factura.")     

                    _conexion.commit() ''' 
                    try:
                        if rpta_pdf > 0:
                            xml_b64 = obtener_xml_b64_por_folio(_token, voucherInfoFolio)

                            if xml_b64 is not None:
                                rpta_xml = insertar_xml(_id_emp, voucherInfoFolio, xml_b64, _conexion)
                                print("Insertó xml:", rpta_xml)
                                filas_afectadas_total += rpta_xml  # AQUI ES CUANDO INSERTO EL REGISTRO ENTERO

                                if rpta_xml < 0:  # retorna -1 en caso de error...
                                    _conexion.rollback()
                            else:
                                #revisar rollback cuando está vacío el pdf
                                _conexion.rollback()

                                print("No se encontró el documento XML o está vacío para el folio:", voucherInfoFolio)
                                # Aquí puedes manejar el caso de que xml_b64 sea None como consideres necesario
                    except Exception as e:
                        _conexion.rollback()
                        print("Fallo la insercion de xml de factura.")

                    _conexion.commit()

     #factura insertada en bd..      
    return filas_afectadas_total

def insertar_cliprov(rut_txt, rut_sin_dv, dv, _conexion):  #buscar e insertar cliente proveedor en la tabla d ela bd...   
    try:
        cursor = _conexion.cursor()        
        cursor.callproc('fn_defontana_buscar_insertar_cliprov', [   
                                                rut_txt,
                                                rut_sin_dv,
                                                dv
                                                ])
        id_cliente = cursor.fetchone()
        id_cliente = id_cliente[0] # Obtener el id cliente                

        if id_cliente == 0: 
            _conexion.rollback()
  

    except Exception as e:
        print("ERROR: {}".format(e))
        with open(ruta_archivo, 'a') as archivo:
            archivo.write("\nError al buscar insertar cliente proveedor en base de datos...")
        id_cliente = -1  # Retorna -1 en caso de error
        
    return id_cliente

def insertar_factura_venta(voucherInfoFolio, _id_emp, id_cliente,fecha_actual,documentType,firstFolio,lastFolio,status,emissionDate,dateTime,expirationDate,
                           contactIndex,paymentCondition,sellerFileId,billingCoin,billingRate,shopId,priceList,giro,city,district,contact,
                           attachedDocumentsDate,attachedDocumentType,attachedDocumentName,attachedDocumentNumber,attachedDocumentTotal,
                           attachedDocumentTotalDocumentTypeId,attachedDocumentFolio,attachedDocumentsReason,attachedDocumentsGloss,gloss,affectableTotal,
                           exemptTotal,taxeCode,taxeValue,ventaRecDesGlobal,total,voucherInfoType,isTransferDocument,timestamp, _conexion): 
    #Inserta factura de venta, si ya existe registro retorna un 2
    try:
        cursor = _conexion.cursor()        
        cursor.callproc('fn_defontana_insertar_factura_venta_2', [  
                            voucherInfoFolio, _id_emp, id_cliente,fecha_actual,documentType,firstFolio,lastFolio,status,emissionDate,dateTime,expirationDate,
                            contactIndex,paymentCondition,sellerFileId,billingCoin,billingRate,shopId,priceList,giro,city,district,contact,
                            attachedDocumentsDate,attachedDocumentType,attachedDocumentName,attachedDocumentNumber,attachedDocumentTotal,
                            attachedDocumentTotalDocumentTypeId,attachedDocumentFolio,attachedDocumentsReason,attachedDocumentsGloss,gloss,affectableTotal,
                            exemptTotal,taxeCode,taxeValue,ventaRecDesGlobal,total,voucherInfoType,isTransferDocument,timestamp
                                                                  ])
        resultado = cursor.fetchone()
        valor_retorno = resultado[0] # 1 se insertó, -1 error, 2 ya existe en bd.      

        #probar que inserte facturas...
       # print("*"*20)
        #print("factura: ",voucherInfoFolio)
        #print("insertada factura: (1 si, 0 no, 2 ya existe en bd) ",valor_retorno)        
       
        if valor_retorno == 0: 
            _conexion.rollback()
        #_conexion.commit()   

    except Exception as e:
        print("ERROR: {}".format(e))
        with open(ruta_archivo, 'a') as archivo:
            archivo.write("\nError al buscar insertar cliente proveedor en base de datos...")
        valor_retorno = -1  # Retorna -1 en caso de error
        
    return valor_retorno

def insertar_detalle_factura_venta (detailLine, voucherInfoFolio,_id_emp,id_cliente,type, code, count,price,isExempt,discountType,discountValue,comment,total,
                                    priceList,analysis,accountNumber,businessCenter,classifier01,classifier02,_conexion):
    try:                        
        cursor = _conexion.cursor() 
        # Insertar detalles de ventas
        cursor.callproc('fn_defontana_insertar_detalle_de_venta_2', [     #la funcion con sufijo '_2' me retorna si existe ya el registro en bd.
                                                detailLine, voucherInfoFolio, #ESTE ES EL FOLIO DE LA FACTURA
                                                _id_emp,id_cliente,type, code, count, price, isExempt, discountType, discountValue, comment,total,priceList,analysis,
                                                accountNumber,businessCenter,classifier01,classifier02
                                                ])
        
        resultado_detalle = cursor.fetchone()        
        valor_retorno_detalle = resultado_detalle[0]

        if valor_retorno_detalle == 0: 
            _conexion.rollback()
        #_conexion.commit()                

    except Exception as e:
        _conexion.rollback()
        print("ERROR: {}".format(e)) 
        with open(ruta_archivo, 'a') as archivo:
            archivo.write("Error al Insertar detalle de factura en la base de datos...") 
        valor_retorno_detalle = -1 #si hay error, retorna -1
    
    
    return valor_retorno_detalle

def obtener_clientes_por_empresa(_token): #con el token identifico en qué empresa estoy.
    url = "https://api.defontana.com/api/sale/GetClients?status=0&itemsPerPage=250&pageNumber={}".format(1) #buscar por número de página
    
    payload = {}
    headers = {
    'Authorization': 'Bearer {}'.format(_token)
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    
    return response

# Insertar o actualizar datos de Clientes de Cada planta
def insertar_actualizar_datos_usuario(_respuesta, _conexion):
    clientList = _respuesta['clientList']
    filas_afectadas_total = 0 #clientes actualizados o insertado
    for cliente in clientList:        
        rut = cliente['legalCode'] #esto servirá para buscar si el cliente ya está insertado..        
        direccion = cliente['address']
        ciudad = cliente['city']
        rubro = cliente['business']
        nombre = cliente['name']
        comuna = cliente['district']
        # rut cliente para tbl_clientes_proveedores
        rut_sin_dv = rut[:-1].replace(".","").replace("-","") # quitar . - dv
        #dv = rut[-1]
        #dv = "0"        
        ''' print (rut)
        print (direccion)
        print (ciudad)
        print(rubro)
        print (nombre)
        print(comuna)
        print(rut_sin_dv)
        print ("****")'''
      
        #buscar e insertar cliente proveedor o actualizar datos:
        try:
            cursor = _conexion.cursor()
           
            cursor.callproc('fn_defontana_insertar_actualizar_datos_cliente', [   
                                                    rut,
                                                    direccion,
                                                    ciudad,
                                                    rubro,
                                                    nombre,
                                                    comuna,
                                                    rut_sin_dv
                                                    #,dv
                                                    ])

            datos_cliente_actualizados = cursor.fetchone()
            datos_cliente_actualizados = datos_cliente_actualizados[0]
            #print (datos_cliente_actualizados)
            #datos_cliente_actualizados = fila_afectada[0] 
            #print("id_cli",id_cliente)            
            # print(id_cliente)
            #_conexion.commit()   
            # Incrementar el valor de filas_afectadas_total por el valor de retorno de la función.
            
            #prueba de datos:
            ''' print(rubro) 
            print(rut)
            print(rut_sin_dv)     
            print(datos_cliente_actualizados)   
            print("****")'''
           
            if datos_cliente_actualizados > 0:
                filas_afectadas_total += datos_cliente_actualizados

            
            #print("filas afectadas datos cli", filas_afectadas_total)    
           
            if datos_cliente_actualizados == 0: 
                _conexion.rollback()            
            #_conexion.commit()     
                    
        except Exception as e:
            _conexion.rollback()
            print("ERROR: {}".format(e))
            with open (ruta_archivo, 'a') as archivo:
                archivo.write("\nError al obtener Actualizar info de Cliente en base de datos...")                    
        _conexion.commit()         
    return filas_afectadas_total

'''def insertar_servicios_de_factura (_id_emp,_respuesta, _conexion):
    serviceList = _respuesta['serviceList']
    # print("sl",serviceList)
    #filas_afectadas_total = 0 #facturas insertadas.
    for saleList in serviceList:
        
        #print("*"*20)
        #print(saleList)
        type = saleList['type']
        active = saleList['active']
        code = saleList['code']
        print("code",code)
        desc = saleList['description']
        #det_desc = saleList['detailedDescription']
        sellprice = saleList['sellPrice']
        unit = saleList['unit']
        unitcost = saleList['unitCost']
        cat_id = saleList['categoryID']
        useot = saleList['useOT']

        #buscar e insertar cliente proveedor en la tabla d ela bd...
        try:
            cursor = _conexion.cursor()
            #cursor.callproc('fnc_defontana_bus_ins_cliprov', [
            cursor.callproc('fn_defontana_insertar_servicio', [   
                                                    _id_emp,
                                                    type,
                                                    active,
                                                    code,
                                                    desc,
                                                    #det_desc,
                                                    sellprice,
                                                    unit,
                                                    unitcost,
                                                    cat_id,
                                                    useot                                    
                                                    ])
            id_serv = cursor.fetchone()
            id_serv = id_serv[0] # Obtener el id cliente
            # print(id_cliente)
            _conexion.commit()
            
           
        except Exception as e:
            print("ERROR: {}".format(e))
            with open (ruta_archivo, 'a') as archivo:
                archivo.write("\nError al obtener ID de Cliente Proveedor desde la base de datos...")
        
        return id_serv
'''
'''def obtener_respuesta_get_services(_url, _item_por_pagina, _numero_pagina, _token):

    url = _url.replace(":item_por_pagina", _item_por_pagina).replace(":numero_pagina", _numero_pagina)
            
    payload = {}

    headers = {
    'Authorization': 'Bearer {}'.format(_token)
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    #print("respuesta completa")
    #print(response.json())
    #print(url)
    return response
'''

'''def obtener_lectura_get_services(_conexion):
    try:
        cursor = _conexion.cursor()
        #cursor.callproc('fnc_defontana_obtener_lectura', [])
        cursor.callproc('fn_defontana_obtener_lectura_get_services', [])
        # print(cursor.fetchall()())        
        return cursor.fetchall()
        
    except Exception as e:
        print("ERROR: {}".format(e))
        with open(ruta_archivo, 'a') as archivo:
            archivo.write("Error al Obtener Lecturas desde la base de datos...")
'''

'''def insertar_servicios_de_empresas(_servicios_de_empresas_endpoints, cone):
    #print("alo")
    # referencia a items en respuesta
    for endpoint in _servicios_de_empresas_endpoints:
        id_emp = endpoint[0]
        max_reintentos = endpoint[1]
        url = endpoint[2]
        item_por_pagina = endpoint[3]
        numero_pagina = endpoint[4]
        token = endpoint[5]
        #print("id_:emopd",id_emp)
               
        respuesta_completa = obtener_respuesta_get_services(url,item_por_pagina,numero_pagina,token) #obtengo la lista de servicios a leer..
        #print("rc",respuesta_completa.json())
        for reintento in range(max_reintentos):                
            flag = None
           
            if respuesta_completa.status_code == 200:
                flag = True
                break                
            else: #respuesta no dio codigo 200..
                items_totales = respuesta_completa.json()['totalItems']
                cod_rpta = respuesta_completa.status_code
                mensaje = respuesta_completa.json()['message']
                """
                print("{} | {} |  Reintento: {} | Código Respuesta: {} | Mensaje: {}\n".format(fecha_actual, reintento+1, cod_rpta, mensaje))
        
                with open(ruta_archivo, 'a') as archivo:
                    archivo.write("{} | {} |  Reintento: {} | Código Respuesta: {} | Mensaje: {}\n".format(fecha_actual,  reintento+1, cod_rpta, mensaje))
                """    
        items_totales = 0
        if flag:
            #fecha_actual = datetime.now().strftime("%d-%m-%Y %H:%M:%S")#strftime("%Y-%m-%d %H:%M:%S")
            fecha_actual = datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f")
            #print("Leyendo Empresa:",nombre_empresa[0])
            items_totales = 0
            
            if respuesta_completa.json()['totalItems'] != 0 :
                items_totales = respuesta_completa.json()['totalItems']
            else : items_totales = 0
            #print ("Filas Respuesta: ", items_totales) 
            
            #id_servicio = 
            insertar_servicios_de_factura(id_emp,respuesta_completa.json(),cone) #insertar_saleList(respuesta_completa.json(), cone, id_emp)  
            #print(id_servicio)
'''

def obtener_nombre_empresa(_conexion, _id_emp):
    try:
        cursor = _conexion.cursor()
       # cursor.callproc('fnc_defontana_obtener_nom_emp', [_id_emp])
        cursor.callproc('fn_defontana_obtener_nom_emp', [_id_emp])
        return cursor.fetchone()
    except Exception as e:
        print("ERROR: {}".format(e))    
        with open(ruta_archivo, 'a') as archivo:
            archivo.write("Error al Obtener Nombre de empresas desde la base de datos...")

# Obtener lista de endpoint de facturas de ventas a leer para llamar a la respuesta
#test1 deberia estar llamando a todos los métodos.
'''def obtener_lectura_facturas_ventas(_conexion):
    try:
        cursor = _conexion.cursor()
        #cursor.callproc('fnc_defontana_obtener_lectura', [])
        cursor.callproc('fn_defontana_obtener_lectura_facturas_ventas', [])
        # print(cursor.fetchall()())        
        return cursor.fetchall()
        
    except Exception as e:
        print("ERROR: {}".format(e))
        with open(ruta_archivo, 'a') as archivo:
            archivo.write("Error al Obtener Lecturas desde la base de datos...")

'''

# Obtener PDF de cada folio
def obtener_b64_por_folio(_token,_folio): #con el token identifico en qué empresa estoy, con el folio busco el PDF
    url = "https://api.defontana.com/api/Sale/GetPDFDocumentBase64?documentType=FVAELECT&folio={}".format(_folio)
   
    #url para NEW PDF:
    #test1 
    #url = "https://api.defontana.com/api/Sale/GetNEWPDFDocumentBase64?documentType=FVAELECT&folio={}".format(_folio)
    
    payload = {}
    headers = {
    'Authorization': 'Bearer {}'.format(_token)
    }

    response = requests.request("GET", url, headers=headers, data=payload)
   
    if response.status_code == 200:
        response_data = response.json()
        # Verificar si el "document" está presente y no es null ni una cadena vacía
        if "document" in response_data and response_data["document"]:
             # Imprimir los valores adicionales
            print("success:", response_data.get("success"))
            print("message:", response_data.get("message"))
            print("exceptionMessage:", response_data.get("exceptionMessage"))           
            
            return response_data["document"]          
    
    # Devolver None o una cadena vacía si "document" no está presente, es null o está vacío
    return None

# Obtener XML de cada folio
def obtener_xml_b64_por_folio(_token,_folio): #con el token identifico en qué empresa estoy, con el folio busco el PDF
    url = "https://api.defontana.com/api/Sale/GetXMLDocumentBase64?documentType=FVAELECT&number={}".format(_folio)

    payload = {}
    headers = {
    'Authorization': 'Bearer {}'.format(_token)
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    
    if response.status_code == 200:
        response_data = response.json()
        # Verificar si el "document" está presente y no es null
        if "document" in response_data and response_data["document"]:
            return response_data["document"]
    
    # Devolver None si "document" no está presente o es null
    return None

#Insertar en bd pdf
def insertar_pdf(_id_emp, _folio, _b64_pdf, _conexion):
    #print (_id_emp)
    #print (_folio)
    #print (_b64_pdf[:10])
    try:
        cursor = _conexion.cursor()
        cursor.callproc('fn_defontana_insertar_factura_b64', [
            _id_emp,
            _folio,
            _b64_pdf
        ])
        _conexion.commit()
        rpta_fn = cursor.fetchone()
        rpta_fn = rpta_fn[0] # Obtener el resultado de la función

        print (rpta_fn)

    except Exception as e:
        print("ERROR: {}".format(e))
        with open(ruta_archivo, 'a') as archivo:
            archivo.write("\nError al insertar pdf desde la base de datos...")
        rpta_fn = -1  # Retorna -1 en caso de error
    
    return rpta_fn

#Insertar en bd xml
def insertar_xml(_id_emp, _folio, _b64_pdf, _conexion):
    try:
        cursor = _conexion.cursor()
        cursor.callproc('fn_defontana_insertar_xml_b64', [
            _id_emp,
            _folio,
            _b64_pdf
        ])
        _conexion.commit()
        rpta_fn = cursor.fetchone()
        rpta_fn = rpta_fn[0] # Obtener el resultado de la función

    except Exception as e:
        print("ERROR: {}".format(e))
        with open(ruta_archivo, 'a') as archivo:
            archivo.write("\nError al insertar xml desde la base de datos...")
        rpta_fn = -1  # Retorna -1 en caso de error
        
    return rpta_fn

#deprecado? repetido? idk
'''# INSERTAR DATOS EN LOG GENERAL
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
'''
# Insertar en log detalle de base de datos
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


def formatear_rut(rut):
    '''# Ejemplo de uso:
    rut = "76.708.710-1"
    rut_formateado = formatear_rut(rut)
    print(rut_formateado)  # Esto imprimirá "76708710"'''
    # Eliminar puntos y guiones y obtener solo los números y el dígito verificador
    rut_formateado = ''.join(filter(str.isdigit, rut))

    # Si el rut formateado tiene más de 1 caracter, eliminar el último que es el dígito verificador
    if len(rut_formateado) > 1:
        rut_formateado = rut_formateado[:-1]

    return rut_formateado

def obtener_id_cliprov(rut, _conexion):
    cursor = _conexion.cursor()
    # Buscar el id_company correspondiente al rut en tbl_clientes_proveedores
    query = "SELECT id_cliprov FROM tbl_clientes_proveedores WHERE rut_no_dv_cliprov = {}".format(rut)
    cursor.execute(query)
    result = cursor.fetchone()

    if result is not None:
        id_company = result[0]       
        return id_company        
    else:
        return None
    
def lista_para_tbl_clientes_proveedores_empresas(json_data, id_emp, _conexion):
    id_tipo_cliprov = 1  #en BD: tbl_tipos_clientes_proveedores: 1 = Cliente, 2 = Proveedor
    
    lista_cli_prov_emp = []

    for data_item in json_data:
        rut = data_item.get("clientFile")  # Obtener el rut del elemento JSON (ESTÁ CON PUNTOS Y GUION)
        
        if rut:
            rut_formateado = formatear_rut(rut) #pasa de rut con puntos guion y dv. A: sin puntos y sin dv.
            id_cliprov = obtener_id_cliprov(rut_formateado, _conexion)

            
            if id_cliprov is not None:
                # Agregar una tupla con los valores a la lista
                lista_cli_prov_emp.append((id_cliprov, id_emp, id_tipo_cliprov))
    
    return lista_cli_prov_emp

def insert_lista_para_tbl_clientes_proveedores_empresas(keys, connection):
    cursor = connection.cursor()
    rows_affected = 0
    for key in keys:
        cursor.callproc('fn_sii_insertar_cliente_proveedor_empresa', key)
        result = cursor.fetchone()
        rows_affected += result[0]
    connection.commit()
    cursor.close()
    return rows_affected  


### end new code


def connection_database():
    database = "OFINANCE"
    user = "integrator"   
    host = '192.168.149.20' 
    password = "aZwY=d@tA79"
    port = 5432

    try:
        conexion = psycopg2.connect(database=database, user=user, host=host, password=password, port=port)
        #print(f"{datetime.now().strftime('%H:%M:%S %d/%m/%Y')} - Conexión a BD exitosa")
        #print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} {datetime.now().strftime('%d/%m/%Y')} - Conexión a BD exitosa")
        return conexion
    except Exception as e:
        print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} {datetime.now().strftime('%d/%m/%Y')} - Error de conexión a BD: {str(e)}")
        return None
   

#funcional, pero deprecado:
def obtener_lecturas_grupo_actual_sin_funcion_bd(conexion, _id_gpo):
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
#funcional, pero deprecado:
def obtener_lecturas_por_grupo_sin_funcion_bd(conexion):

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



#con funcion en base de datos.
def obtener_lecturas_por_grupo(conexion):
    cursor = conexion.cursor()
    try:
        cursor.callproc('fn_defontana_listar_lecturas_por_grupo')
        
        #test 1 
        #hardcodeado!
        #cursor.callproc('fn_defontana_listar_lecturas_por_grupo_sologetcaf')
        
        # Ahora la función devuelve todas las filas como una lista de tuplas
        filas = cursor.fetchall()
        # Obtenemos los nombres de las columnas
        column_names = [desc[0] for desc in cursor.description]
        
        lecturas_por_grupo = {}
        for fila in filas:
            # Convierte cada fila en un diccionario
            lectura = dict(zip(column_names, fila))
            # El ID del grupo está en la última columna (de acuerdo a tu estructura actual)
            id_grupo_lectura = fila[-1]
            
            # Si aún no hay una entrada para este ID de grupo, crea una lista vacía
            if id_grupo_lectura not in lecturas_por_grupo:
                lecturas_por_grupo[id_grupo_lectura] = []
            # Añade el diccionario de lectura a la lista de lecturas para este ID de grupo
            lecturas_por_grupo[id_grupo_lectura].append(lectura)

        return lecturas_por_grupo
    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")
        return None
    finally:
        cursor.close()

#con funcion en base de datos.
def obtener_lecturas_grupo_actual(conexion, _id_gpo):
    cursor = conexion.cursor()
    try:
        cursor.callproc('fn_defontana_listar_lecturas_grupo_actual', [_id_gpo])

        lecturas = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        return [dict(zip(column_names, row)) for row in lecturas]
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        cursor.close()


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

def manejar_grupo(id_grupo, tipos_lectura):
#def manejar_grupo(id_grupo):
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
        
        #procesar_grupo_obtener_lecturas(id_grupo,called_execution_time)
        procesar_grupo_obtener_lecturas(id_grupo,called_execution_time,tipos_lectura)
        
        
        print(f"Ejecutado proceso para el grupo {id_grupo} a las {hora_ejecucion.strftime('%H:%M:%S')}")

def procesar_factura_venta(lectura, ruta_archivo, ID_LOG, hora_orquestado):
        #def procesar_factura_venta(lectura, ruta_archivo, ID_LOG):
    fch_actual = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    #print ("Entre a procesar_factura_venta a las "+fch_actual)

    id_lectura = lectura['id_lectura']
   
    ID_LECTURA_DETA = id_lectura

    id_emp = lectura['id_emp']
    max_reintentos = lectura['max_reintentos']
    url = lectura['url']
    endpoint = lectura['endpoint']
    item_por_pagina = lectura['item_por_pagina']
    numero_pagina = lectura['numero_pagina']
    token = lectura['token']
    id_gpo = lectura['id_gpo']

    resta_mes_periodo = lectura['resta_mes_periodo']   

    log_filename = os.path.join(log_directory, f"log_grupo_lectura_{id_gpo}.txt")

    ruta_archivo = log_filename

    cone = connection_database() 
    if cone is None:
        print("ERROR BD")
        with open(ruta_archivo, "a") as archivo:                   
            archivo.write( "\n***** ERROR CONEXIÓN BD!!!!: "+str(datetime.now())+" de COMPRAS SII:")
    else:
        
        try:
            fecha_actual = datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f")
            #print ("Testeo")

            OK_LECT_GENE = 0
            FAIL_LECT_GENE = 0

            ID_LECTURA_DETA = id_lectura

            ID_ESTATUS_DETA = INICIO_OK

            ID_LOG_DETA = ID_LOG

            MENSAJE_COMPLETO_GEN = 'Ok'

            #FCH_INICIO_LECT_GENE = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            FCH_INICIO_LECT_GENE = datetime.now().strftime('%Y-%m-%d ') + hora_orquestado# fch_dato en tbl_log
            #print ("fch inicio lect:"+ str(FCH_INICIO_LECT_GENE))

            ID_ESTATUS_DETA = ENDPOINTS_OK

            nombre_empresa = obtener_nombre_empresa(cone, id_emp)

            total_filas_leidas = 0
            total_filas_insertadas = 0

            CANT_REINTENTOS_DETA = 1

            '''CANT_LECT_GENE = len(endpoints_por_leer)
            ID_LOG_DETA = insertar_datos_log(cone)'''

            # Llamada inicial para obtener el total de páginas            
            #respuesta_inicial = obtener_respuesta(url, item_por_pagina, numero_pagina, token)
            respuesta_inicial = obtener_respuesta(url, item_por_pagina, numero_pagina, token, resta_mes_periodo)
            
            
     
            '''intento = 0
            for intento in range(max_reintentos):
                print ("Intento: "+str(intento))
                 
                #guardo el reintento que me tiró datos
                CANT_REINTENTOS_DETA_ventas = intento
                
                FCH_INICIO_LECT_DETA_ventas = datetime.now().strftime("%d-%m-%Y %H:%M:%S") '''
           
            FCH_INICIO_LECT_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 
            #test1
            print ("FCH_INICIO_LECT_DETA: "+ str(FCH_INICIO_LECT_DETA) )


            contador = 1

            if respuesta_inicial.status_code == 200:
                    MENSAJE_COMPLETO_GEN = ''
                    
                    #INICIO DE LECTURA
                    FCH_DATO_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 


                    total_items = respuesta_inicial.json()['totalItems']
                    total_paginas = calcular_total_paginas(total_items, int(item_por_pagina))
                    
                    print ("total items recibido "+ str(total_items))
                    print ("total paginas a recorrer "+ str(total_paginas))

                    # Procesa cada página
                    for numero_pagina_actual in range(1, total_paginas + 1):
                        #print ("entre a procesar paginas")
                        print ("num pag actual: " + str(numero_pagina_actual))

                        #respuesta_completa = obtener_respuesta(url, item_por_pagina, str(numero_pagina_actual), token)
                        respuesta_completa = obtener_respuesta(url, item_por_pagina, str(numero_pagina_actual), token, resta_mes_periodo)


                        # Aquí comienza la lógica de procesamiento de respuesta_completa
                        if respuesta_completa.status_code == 200:
                           
                            items_totales = respuesta_completa.json()['totalItems']

                            total_filas_leidas += items_totales  # Acumula el total de filas leídas


                            if items_totales > 0: #si hay al menos un registro, intento insertar a la tabla de rompimiento.-

                                lista_tbl_clientes_proveedores_empresas = lista_para_tbl_clientes_proveedores_empresas(respuesta_completa.json()['saleList'], id_emp, cone)
                                
                                filas_insertadas_tbl_cli_prov_emp = insert_lista_para_tbl_clientes_proveedores_empresas(lista_tbl_clientes_proveedores_empresas, cone)
                        
                                if filas_insertadas_tbl_cli_prov_emp > 1 :
                                    print("Nuevos Clientes en tbl_clientes_proveedores_empresas: "+str(filas_insertadas_tbl_cli_prov_emp))
                                    with open(ruta_archivo, "a") as archivo:                   
                                        archivo.write( "\nNuevos Clientes en tbl_clientes_proveedores_empresas: "+str(filas_insertadas_tbl_cli_prov_emp+ "\n") )                      
                            
                                #envío el token, porque lo ocupo para el pdf y xml..   
                                
                                #INSERTAR DATOS DE FACTURAS
                                facturas_insertadas = insertar_saleList(respuesta_completa.json(), cone, id_emp, token)   
                                
                                total_filas_insertadas += facturas_insertadas  # Acumula el total de filas insertadas


                                cod_rpta = respuesta_completa.status_code
                                mensaje = respuesta_completa.json()['message']


                                with open (ruta_archivo, 'a') as archivo:
                                    archivo.write("{} | Página {} | {} | Filas Insertadas: {} | Filas Respuesta: {}  | Código Respuesta {} | {} \n".format(fecha_actual,numero_pagina_actual, nombre_empresa[0],facturas_insertadas,items_totales, cod_rpta, mensaje) )   
                                contador += 1
                                print("{} | Página {} | {} | Filas Insertadas: {} | Filas Respuesta: {}  | Código Respuesta {} | {} \n".format(fecha_actual, numero_pagina_actual, nombre_empresa[0],facturas_insertadas,items_totales, cod_rpta, mensaje) )   
                            

                                CANT_READ_DETA = items_totales
                                CANT_INSERT_DETA = facturas_insertadas
                                FCH_DATO_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 
                                FCH_FIN_LECT_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                                
                                #ID_ESTATUS_DETA = RESPUESTA_OK 
                                ID_ESTATUS_DETA = FIN_OK
                                
                                #esto debo areglarlo para que el total de lecturas tambien se sume.
                                OK_LECT_GENE += 1
                                print ("*****")

                                pass
                            

                        else:
                            CANT_REINTENTOS_DETA += 1 #se suma un reintento.

                            MENSAJE_COMPLETO_GEN = 'Error'

                            print("Error en respuesta..") #romper la actulizacion lectura.
                            # Fecha actual
                            '''fecha_actual_str = datetime.now().strftime("%d-%m-%Y %H:%M:%S")    

                            ventas_insertadas = 0
                            filas_afectadas = 0'''

                            FCH_DATO_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 
                            FCH_FIN_LECT_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 
                            CANT_READ_DETA = 0
                            CANT_INSERT_DETA = 0
                            
                            ID_ESTATUS_DETA = RESPUESTA_FAIL
                            print ("id_status_deta: " + str(ID_ESTATUS_DETA))
                            #ID_ESTATUS_DETA = RESPUESTA_ALERT #al menos una respuesta entró en error.

                            #INSERCIÓN BD tabla tbl_log_detalle_sii
                            #insert_log_det_fail = insertar_datos_log_detalle_defontana(cone, ID_LOG_DETA, ID_ESTATUS_DETA, FCH_DATO_DETA, FCH_INICIO_LECT_DETA, FCH_FIN_LECT_DETA, CANT_READ_DETA, CANT_INSERT_DETA, ID_LECTURA_DETA, CANT_REINTENTOS_DETA)
                            
                            #test1 16-11 19.28
                            insert_log_det_fail = insertar_datos_log_detalle_defontana(cone, ID_LOG_DETA, ID_ESTATUS_DETA, FCH_DATO_DETA, FCH_INICIO_LECT_DETA, FCH_FIN_LECT_DETA, total_filas_leidas, total_filas_insertadas, ID_LECTURA_DETA, CANT_REINTENTOS_DETA)
                           
                            print ("insert log det fail: ",str(insert_log_det_fail))
                            FAIL_LECT_GENE += 1
                     
                    ID_ESTATUS_DETA = FIN_OK
                   # FCH_DATO_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 

                    FCH_FIN_LECT_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 

                    # Inserción de un único registro de detalle después de procesar todas las páginas
                    insert_log_detalle_ok = insertar_datos_log_detalle_defontana(cone, ID_LOG_DETA, ID_ESTATUS_DETA, FCH_DATO_DETA, FCH_INICIO_LECT_DETA, FCH_FIN_LECT_DETA, total_filas_leidas, total_filas_insertadas, ID_LECTURA_DETA, CANT_REINTENTOS_DETA)
                    #print("insert log det ok: ", str(insert_log_detalle_ok))

                            
            else :
                MENSAJE_COMPLETO_GEN = 'Error'

            #aqui finalizar la lectura del log.
            #fecha_actual = datetime.now().strftime("%d-%m-%Y %H:%M:%S")#strftime("%Y-%m-%d %H:%M:%S")
            fecha_fin = datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f")

            # Fecha actual
            fecha_actual_str = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

            COMENT_LECT_GENE = MENSAJE_COMPLETO_GEN

            if FAIL_LECT_GENE == CANT_LECT_GENE:
                COMENT_LECT_GENE = "API CON FALLA TOTAL"
            #else:
            #    COMENT_LECT_GENE = "TERMINADO OK"
            
            FCH_FIN_LECT_GENE = datetime.now().strftime("%d-%m-%Y %H:%M:%S")  

            # actualizar datos de tbl_log
            actualizar_log(cone, ID_LOG_DETA, FCH_INICIO_LECT_GENE, FCH_FIN_LECT_GENE, CANT_LECT_GENE, OK_LECT_GENE, FAIL_LECT_GENE, COMENT_LECT_GENE)

            cant_lect = 0 #filas totales
            ok_lect  = 0 #filas insertadas
            fail_lect = 0 #filas con fallas  

            with open(ruta_archivo, 'a') as archivo:
                archivo.write("*** FIN LECTURA FACTURAS VENTAS: {} ***\n\n".format(fecha_actual) )   
            cone.commit()
            cone.close()                

          
            # ...
            
            #print("fin main") 




        except Exception as e:
            import traceback
            error_msg = traceback.format_exc()
            print("Error:\n", error_msg)
    


def procesar_caf(lectura, ruta_archivo, ID_LOG, hora_orquestado):
    #print ("entre al procesar caf")
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
    #print ('response antes de.. '+ str(response.json()))
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

            #FCH_INICIO_LECT_GENE = datetime.now().strftime("%d-%m-%Y %H:%M:%S") # fch_dato en tbl_log
            FCH_INICIO_LECT_GENE = datetime.now().strftime('%Y-%m-%d ') + hora_orquestado# fch_dato en tbl_log



            CANT_LECT_GENE = 1 #hardcodeado, deberia ser if itemcaf 1 a 1.

            ID_LOG_DETA = ID_LOG #ya venia la id log, por ende no deberia volver a calcular.
            #print ("id_log",ID_LOG_DETA)

            ID_LECTURA_DETA = id_lectura

            ID_ESTATUS_DETA = ENDPOINTS_OK #LECTURAS DE METODOS A LEER OBTENIDAS        
            MENSAJE_COMPLETO_GEN = ''

            CANT_REINTENTOS_DETA = 0 #hardcodeado, debo mejorarlo.
            FCH_INICIO_LECT_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 

            CANT_READ_DETA = CANT_LECT_GENE #A ?
            CANT_INSERT_DETA = 1#resultado #hardcoeasdosdodo


            #pasar aqui la hora orquestada.
            #FCH_DATO_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 
            
            #FCH_DATO_DETA = fch_orquestada
             # Añadir la fecha actual al tiempo
            FCH_DATO_DETA = datetime.now().strftime('%Y-%m-%d ') + hora_orquestado

            
            FCH_FIN_LECT_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            
            ID_ESTATUS_DETA = FIN_OK

            #print ("CANT_READ_DETA ", str(items_totales))
            #print ("CANT_INSERT_DETA ", str(facturas_insertadas))

            insert_log_detalle_ok = insertar_datos_log_detalle_defontana(conexion, ID_LOG_DETA, ID_ESTATUS_DETA, FCH_DATO_DETA, FCH_INICIO_LECT_DETA, FCH_FIN_LECT_DETA, CANT_READ_DETA, CANT_INSERT_DETA, ID_LECTURA_DETA, CANT_REINTENTOS_DETA )
            #print ("insert log det ok: ", str(insert_log_detalle_ok))
            #OK_LECT_GENE += 1     
            OK_LECT_GENE = 1 #hardcodeado?
            FAIL_LECT_GENE = 0 #HARDCOD
            COMENT_LECT_GENE = ' '

            #if FAIL_LECT_GENE == CANT_LECT_GENE:
            #    COMENT_LECT_GENE = "API CON FALLA TOTAL"
            FCH_FIN_LECT_GENE = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            actualizar_log(conexion, ID_LOG_DETA, FCH_INICIO_LECT_GENE, FCH_FIN_LECT_GENE, CANT_LECT_GENE, OK_LECT_GENE, FAIL_LECT_GENE, COMENT_LECT_GENE)
             

            conexion.close()
        else:
            print("La respuesta no contiene la clave 'itemsCaf'")
    else:
        print("El código de estado no es 200")

def insertar_caf_en_bd(conexion, item):
    cursor = conexion.cursor()
    try:
        #result = cursor.callproc("fn_defontana_insertar_caf", [
        result = cursor.callproc("fn_defontana_insertar_o_actualizar_caf", [            
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
        print ("Resultado de fn_defontana_insertar_o_actualizar_caf: ", str(result[0]))
        #print (result)
        #print (result[0])
        return result[0]  # Retorna el resultado de la función (1 para éxito, -1 para error)
    except Exception as e:
        print(f"Error al insertar en la base de datos: {str(e)}")
        return None
    finally:
        cursor.close()


def cargar_tipos_lectura(id_serv):
    tipos_lectura = {}
    # Conectar con la base de datos y ejecutar una consulta 
    # para obtener los id_tipo_lect y su correspondiente nombre 
    # para el id_serv especificado.

    conexion = connection_database()  # Conexión a la base de datos
    cursor = conexion.cursor()
    query = "SELECT id_tipo_lect, nom_lect FROM tbl_tipos_lecturas WHERE id_serv = %s"
    cursor.execute(query, (id_serv,))
    
    for id_tipo_lect, nom_lect in cursor.fetchall():
        tipos_lectura[id_tipo_lect] = nom_lect

    cursor.close()
    conexion.close()

    return tipos_lectura


#def procesar_lectura(lectura, log_filename, ID_LOG, called_execution_time):
#def procesar_lectura(lectura, ruta_archivo,ID_LOG):    
#def procesar_lectura(lectura, ruta_archivo, called_execution_time):
#def procesar_lectura(lectura, ruta_archivo):

def procesar_lectura(lectura, log_filename, ID_LOG, called_execution_time,tipos_lectura):

    #tipo_lectura = lectura['endpoint']  # Asumiendo que 'endpoint' contiene la información de tipo de factura
    id_tipo_lectura = lectura['id_tipo_lect']

    # Verificar si el id_tipo_lectura es válido
    if id_tipo_lectura not in tipos_lectura:
        print(f"Tipo de lectura desconocido no encontrado en tabla tbl_tipos_lecturas, id_lectura: {lectura['id_lectura']}")
        
        with open(ruta_archivo, "a") as archivo:                   
            archivo.write(f"Tipo de lectura desconocido no encontrado en tabla tbl_tipos_lecturas, id_lectura: {lectura['id_lectura']}")
        return
    

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
       
            #if tipo_lectura == "facturas_ventas":    
            if id_tipo_lectura == 5: #en bd: nom_lect= FACTURAS VENTA & desc_lect = Obtener Facturas desde API Defontana
                #procesar_factura_venta(lectura, ruta_archivo, ID_LOG)#print ()
                #print("id_tipo_lectura: "+str(id_tipo_lectura))
                procesar_factura_venta(lectura, ruta_archivo, ID_LOG, called_execution_time)
            
            #elif tipo_lectura == "get_caf": 
            elif id_tipo_lectura == 6:#en bd: nom_lect= CODIGO ASIGNACION DE FOLIOS FACTURAS & desc_lect = Obtener CAF desde API Defontana
                #procesar_caf(lectura, ruta_archivo, ID_LOG)
                procesar_caf(lectura, ruta_archivo, ID_LOG, called_execution_time)
                #print("id_tipo_lectura: "+str(id_tipo_lectura))
            
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


def procesar_grupo_obtener_lecturas(id_grupo,called_execution_time,tipos_lectura):
#def procesar_grupo_obtener_lecturas(id_grupo):
#def procesar_grupo_obtener_lecturas(id_grupo,called_execution_time):    

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
                procesar_lectura(lectura, log_filename, ID_LOG, called_execution_time,tipos_lectura)

                #procesar_lectura(lectura, log_filename, ID_LOG, called_execution_time)

                #procesar_lectura(lectura, log_filename,called_execution_time)

                #procesar_lectura(lectura, log_filename)#, ID_LOG)

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

def insertar_datos_log_caf(_conexion, called_execution_time):  
#def insertar_datos_log_caf(_conexion):  
    cursor = _conexion.cursor()
    
    # Añadir la fecha actual al tiempo
    FCH_DATO_GENE = datetime.now().strftime('%Y-%m-%d ') + called_execution_time

    
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
    '''   print("fn_log_detalle_insertar_datos_defontana datos:")
        print(_id_log)
        print(_status)
        print(_fch_dato_det)
        print(_fch_inicio_lect_det)
        print(_fch_fin_lect_det)
        print(_cant_read)
        print(_cant_insert)
        print(_id_lectura)
        print(_cant_reintentos)'''


    _conexion.commit()
    id_log = cursor.fetchone()
    id_log = id_log[0]
    #print ("id log de fn_log_detalle_insertar_datos_defontana " +str(id_log))

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
        # Cuando busco los grupos iniciales, traer los grupos de lectura.

        tipos_lectura = cargar_tipos_lectura(ID_SERVICIO_GENE)  # Cargar los tipos de lectura

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

        #t = Thread(target=manejar_grupo, args=(id_grupo,))
        t = Thread(target=manejar_grupo, args=(id_grupo, tipos_lectura))
        
        print(f"Lanzando hilo para el grupo {id_grupo}")
        active_threads[id_grupo] = t
        t.start()

if __name__ == "__main__":
    main()