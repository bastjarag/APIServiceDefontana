
'''from datetime import datetime, timedelta
import json
import psycopg2
import requests
import os
import sys
import getpass'''

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

# Datos de log en la base de datos (tbl_status_log)
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

interval_minutes = 30
##FIN DATOS TBL STATUS LOG

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

'''if usuario_actual == 'WIN-869S8VCILTL$': #estoy en el servidor windows server para correr el servicio de python...
        ruta_archivo = os.path.join("C:\\Users\\Administrador\\Desktop",nombre_log)
else:
    ruta_archivo = os.path.join("C:\\Users", usuario_actual, "Desktop", nombre_log) '''
# Verificar si el archivo existe, y si no, crearlo
if not os.path.isfile(ruta_archivo):
    with open(ruta_archivo, 'w'):
        pass  # Crea el archivo vacío
######fin config.


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


#llamada de facturas de ventas
def obtener_respuesta(_url, _item_por_pagina, _numero_pagina, _token):
    
    # Obtener la fecha actual en formato de cadena "YYYY-MM-DD"
    fecha_actual_str = datetime.now().strftime("%Y-%m-%d")

    # Convertir la cadena en un objeto datetime
    fecha_actual = datetime.strptime(fecha_actual_str, "%Y-%m-%d")
    #print(fecha_actual)
    #fecha_actual_str = "2023-08-31"

    # Calcular la fecha de 5 días atrás
    fecha_5_dias_atras = fecha_actual - timedelta(days=5)
    
    #fecha_5_dias_atras = fecha_actual - timedelta(days=30)
    
    # Convertir la fecha de 5 días atrás en formato de cadena "YYYY-MM-DD"
    fecha_5_dias_atras_str = fecha_5_dias_atras.strftime("%Y-%m-%d")
   
    # reemplazar or lectura máxima 5 días atras....
    #fecha_5_dias_atras_str = "2023-08-01"
    
    # Luego, convierte las fechas a cadenas antes de usarlas en la URL
    url = _url.replace(":fch_inicio", fecha_5_dias_atras_str).replace(":fch_fin", fecha_actual_str).replace(":item_por_pagina", _item_por_pagina).replace(":numero_pagina", _numero_pagina)
        
    #print (url)

    payload = {}

    headers = {
    'Authorization': 'Bearer {}'.format(_token)
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    #print("respuesta completa")
    #print(response.json())
    return response

def insertar_saleList(_respuesta, _conexion, _id_emp, _token):
    saleLists = _respuesta['saleList']

    filas_afectadas_total = 0 #facturas insertadas.
    for saleList in saleLists:
        
        documentType = saleList['documentType']
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

            ###inserción de b64 pdf a cada folio...
            #print("alo")
        
        # Datos de inventoryInfo en JSON     NO QUIERE GUARDAR ESTO ENDER...
        '''
        inventoryInfo = saleList['inventoryInfo'] # json
        inventoryInfo = json.dumps(inventoryInfo)
        
        # Datos de customFields en JSON    
        customFields = saleList['customFields'] # json
        customFields = json.dumps(customFields)
        
        # Datos de exportData en JSON    
        exportData = saleList['exportData'] # json
        exportData = json.dumps(exportData)
        '''
        isTransferDocument = saleList['isTransferDocument']
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
                    try:
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
                        print("Fallo la insercion de factura.")      

                    #insertar xml por folio y empresa...
                    try:
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

                    _conexion.commit()  

     #factura insertada en bd..      
    return filas_afectadas_total

#factura_existe_en_bd = 0

def insertar_saleList_2(_respuesta, _conexion, _id_emp, _token):
    facturas_existentes_en_bd = 0
    
    saleLists = _respuesta['saleList']

    filas_afectadas_total = 0 #facturas insertadas.
    for saleList in saleLists:
        
        documentType = saleList['documentType']
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

        for attachedDocument in attachedDocuments: #info de cada documento.           
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
        ventaRecDesGlobal = saleList['ventaRecDesGlobal']
        total = saleList['total']
        
        # Datos dentro de voucherInfo en JSON
        voucherInfoFolio = None
        voucherInfoType = None
        voucherInfo = saleList['voucherInfo'] # json

        for  voucherinfos in voucherInfo:
            voucherInfoFolio = voucherinfos['folio'] # PK de tbl_datos_defontana_ventas_v2
            voucherInfoType = voucherinfos['type']
                     
        isTransferDocument = saleList['isTransferDocument']
        timestamp = saleList['timestamp']        
        
        # buscar rut cliente en tbl_clientes_proveedores
        rut_sin_dv = clientFile[:-1].replace(".","").replace("-","") # quitar . - dv
        dv = clientFile[-1]
        id_cliente = None
        
       
        ################################
        ######INSERCIÓN DE DATOS########
        ################################


        id_cliente = insertar_cliprov(clientFile, rut_sin_dv, dv, _conexion) #clientFile es el rut con puntos y con dv con guion 
        #retorna -1 si hubo error y no intenta insertar factura:   
        if id_cliente > 0:         
           #si obtengo la id del cliente insertado o encontrado, intentaré insertar la factura: 

            fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            valor_retorno = insertar_factura_venta(voucherInfoFolio, _id_emp, id_cliente,fecha_actual,documentType,firstFolio,lastFolio,status,emissionDate,dateTime,expirationDate,
                                                    contactIndex,paymentCondition,sellerFileId,billingCoin,billingRate,shopId,priceList,giro,city,district,contact,
                                                    attachedDocumentsDate,attachedDocumentType,attachedDocumentName,attachedDocumentNumber,attachedDocumentTotal,
                                                    attachedDocumentTotalDocumentTypeId,attachedDocumentFolio,attachedDocumentsReason,attachedDocumentsGloss,gloss,affectableTotal,
                                                    exemptTotal,taxeCode,taxeValue,ventaRecDesGlobal,total,voucherInfoType,isTransferDocument,timestamp, _conexion)

            if valor_retorno == 2: #factura ya existe en bd, sumar a la variable que me cuenta las facturas en bd.
                facturas_existentes_en_bd = facturas_existentes_en_bd +1                        
                    

            details = saleList['details'] #items de cada factura...

            #valor_retorno_detalle = 0 #para retornar el valor de detalles insertados, consultado despues...

            if valor_retorno > 0: 
               
                for detail in details: #recorro cada detalle de factura..
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
                    
                    #inserto cada detalle de factura...
                    valor_retorno_detalle = insertar_detalle_factura_venta( detailLine,
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
                                                                classifier02,
                                                                _conexion)     
                
                #fin ciclo para detalle de cada de factura.             

                    
                #print ("valor_retorno_detalle "+str(valor_retorno_detalle))
                              
               
                if valor_retorno_detalle == 1:   #inserto xml y pdf SÓLO SI INSERTÉ RECIEN LA FACTURA!                  
                    try:                        
                        b64 = obtener_b64_por_folio(_token,voucherInfoFolio)   
                        #print (b64 )   
                        print ("id empresa: ",str(_id_emp))
                        print ("factura numero: ",str(voucherInfoFolio)  )             
                        rpta_pdf = insertar_pdf(_id_emp, voucherInfoFolio, b64, _conexion)
                        print("Insertó pdf:",rpta_pdf)
                        if rpta_pdf < 0 : #retorna -1 en caso de error...
                            _conexion.rollback()
                    except Exception as e:
                        _conexion.rollback()
                        print("Fallo la insercion de pdf factura.")  
                        #break    

                    #insertar xml por folio y empresa...
                    try:
                        xml_b64 = obtener_xml_b64_por_folio(_token,voucherInfoFolio)   
                        rpta_xml = insertar_xml(_id_emp, voucherInfoFolio, xml_b64, _conexion)
                        print ("Insertó xml:", rpta_xml)
                        filas_afectadas_total +=  rpta_xml #AQUI ES CUANDO INSERTO EL REGISTRO ENTERO

                        if rpta_xml < 0 : #retorna -1 en caso de error...
                            _conexion.rollback()
                    except Exception as e:
                        _conexion.rollback()
                        print("Fallo la insercion de xml de factura.")     

                    _conexion.commit()  #recién insertado el cliente, factura, detalle, pdf y xml HAGO COMMIT, antes no, hago rollback
        
    
   
    print("Id empresa: ",_id_emp ,"Facturas ya existentes en bd: ",facturas_existentes_en_bd)

     #facturas insertadas en bd..      
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

def insertar_servicios_de_factura (_id_emp,_respuesta, _conexion):
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

def obtener_respuesta_get_services(_url, _item_por_pagina, _numero_pagina, _token):
       
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

def obtener_lectura_get_services(_conexion):
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
# Obtener links para llamar a los endpoint de get services...
def obtener_lectura_get_services(_conexion):
    try:
        cursor = _conexion.cursor()
        cursor.callproc('fn_defontana_obtener_lectura_get_services', [])                
        return cursor.fetchall()
    
    ##funcion para insertar servicios fn_defontana_insertar_servicio
        
    except Exception as e:
        print("ERROR: {}".format(e))
        with open(ruta_archivo, 'a') as archivo:
            archivo.write("Error al Obtener Lecturas de servicios desde la base de datos...")            

def insertar_servicios_de_empresas(_servicios_de_empresas_endpoints, cone):
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
def obtener_lectura_facturas_ventas(_conexion):
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

def obtener_b64_por_folio(_token,_folio): #con el token identifico en qué empresa estoy, con el folio busco el PDF
    url = "https://api.defontana.com/api/Sale/GetPDFDocumentBase64?documentType=FVAELECT&folio={}".format(_folio)
   
   #url para fardela:
   # url = "https://api.defontana.com/api/Sale/GetNEWPDFDocumentBase64?documentType=FVAELECT&folio={}".format(_folio)
    
    payload = {}
    headers = {
    'Authorization': 'Bearer {}'.format(_token)
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code == 200:
    # Convertir la respuesta JSON a un diccionario Python
        response_data = response.json()
   
    document_b64 ='void'
    # Verificar si el "document" está presente en la respuesta
    if "document" in response_data:
        document_b64 = response_data["document"] 

    return document_b64

####obtener XML de cada folio...
def obtener_xml_b64_por_folio(_token,_folio): #con el token identifico en qué empresa estoy, con el folio busco el PDF
    url = "https://api.defontana.com/api/Sale/GetXMLDocumentBase64?documentType=FVAELECT&number={}".format(_folio)

    payload = {}
    headers = {
    'Authorization': 'Bearer {}'.format(_token)
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    if response.status_code == 200:
    # Convertir la respuesta JSON a un diccionario Python
        response_data = response.json()
   
    xml_b64 ='void'
    # Verificar si el "document" está presente en la respuesta
    if "document" in response_data:
        xml_b64 = response_data["document"] 

    return xml_b64

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

def fn_insertar_datos_log_inicio(_id_servicio, _fch_dato,  _conexion):
    #flag = True
    try:
        cursor = _conexion.cursor()
        #cursor.callproc('fnc_defontana_bus_ins_cliprov', [
        cursor.callproc('insertar_datos_log_inicio', [   
                                            _id_servicio,
                                            _fch_dato                                            
                                            ])
        result = cursor.fetchall()  # Obtener todos los resultados
        id_log = result[0][0]  # Obtener el primer valor del primer resultado

        if id_log == 0: 
          _conexion.rollback()
        else:
            return id_log

    except Exception as e:
        _conexion.rollback()
        print("ERROR: {}".format(e))
        with open (ruta_archivo, 'a') as archivo:
            archivo.write("\nError al Obtener el id de log en la base de datos...")    

def fn_insertar_datos_log_actualiza(_id_log, _id_servicio, _fch_inicio_lect, _fch_fin_lect, _cant_lect, _ok_lect , _fail_lect, _coment_lect,  _conexion):
    #flag = True
    try:
        cursor = _conexion.cursor()
        #cursor.callproc('fnc_defontana_bus_ins_cliprov', [
        cursor.callproc('insertar_datos_log_actualiza', [   
                                            _id_log,
                                            _id_servicio,
                                            _fch_inicio_lect, 
                                            _fch_fin_lect, 
                                            _cant_lect, 
                                            _ok_lect,
                                            _fail_lect,
                                            _coment_lect                                                                                       
                                            ])
        log_actualizado = cursor.fetchall()
        log_actualizado = log_actualizado[0][0] # Obtener el id de log escrito

        if log_actualizado == 0: 
          _conexion.rollback()
        else:
            return log_actualizado

    except Exception as e:
        _conexion.rollback()
        print("ERROR: {}".format(e))
        with open (ruta_archivo, 'a') as archivo:
            archivo.write("\nError al Obtener el id de log en la base de datos...")    

def fn_insertar_datos_log_detalle(_id_log, _id_estatus, _fch_dato, _fch_inicio_lect, _fch_fin_lect, _cant_read, _cant_insert, _conexion):
    #flag = True
    try:
        cursor = _conexion.cursor()
        #cursor.callproc('fnc_defontana_bus_ins_cliprov', [
        cursor.callproc('insertar_datos_log_detalle', [   
                                            _id_log, 
                                            _id_estatus,
                                            _fch_dato, 
                                            _fch_inicio_lect,
                                            _fch_fin_lect,
                                            _cant_read,
                                            _cant_insert                                                                                 
                                            ])
        
        log_actualizado = cursor.fetchall()
        log_actualizado = log_actualizado[0][0] # Obtener el id de log escrito

        if log_actualizado == 0: 
          _conexion.rollback()
        else:
            return log_actualizado

    except Exception as e:
        _conexion.rollback()
        print("ERROR: {}".format(e))
        with open (ruta_archivo, 'a') as archivo:
            archivo.write("\nError al insertar detalle de log...")              

def existe_factura_en_bd(_id_emp, _folio, _conexion):
    try:
        query = """SELECT
                    CASE WHEN EXISTS (
                        SELECT 1
                        FROM public.tbl_datos_defontana_ventas
                        WHERE id_emp = {}
                        AND num_doc_venta = {}
                        LIMIT 1
                    )
                    THEN 1
                    ELSE 0
                    END AS registro_existe;
                """.format(_id_emp, _folio)

        cursor = _conexion.cursor()
        cursor.execute(query)
        result = cursor.fetchone()  # Obtener el resultado de la consulta

        if result[0] == 1:
            return True
        else:
            return False

    except Exception as e:
        _conexion.rollback()
        print("ERROR: {}".format(e))
        with open(ruta_archivo, 'a') as archivo:
            archivo.write("\nError al buscar factura en bd...")

    return False  # Si ocurre una excepción, retornar False por defecto

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
# Fin actualizar datos del log en base de datos


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
# Fin insertar en log detalle de base de datos

### 
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

###


def main():
    try:

        OK_LECT_GENE = 0
        FAIL_LECT_GENE = 0

        '''fch_dato_log = time # fch_dato en tbl_log
        cant_lect_log = 0  # cant_lect en tbl_log
        ok_lect_log = 0  # ok_lect en tbl_log
        fail_lect_log = 0  # fail_lect en tbl_log
        coment_lect_log = ""  # coment_lect en tbl_log
        ### PARA EL LOG'''

        print("Main")
        
        cone = conexion_base_datos()
        if cone is not None:

            ID_ESTATUS_DETA = INICIO_OK

            #ID_LOG_DETA = insertar_datos_log(cone) # Insertar datos en tbl_log y devolver su id

            #empresas_endpoint = obtener_lectura_facturas_ventas(cone)

            #CANT_LECT_GENE = len(empresas_endpoint)

            FCH_INICIO_LECT_GENE = datetime.now().strftime("%d-%m-%Y %H:%M:%S") # fch_dato en tbl_log

            print("Entre al Main con conexión a la bd.")
            #fecha_actual = datetime.now().strftime("%d-%m-%Y %H:%M:%S")#strftime("%Y-%m-%d %H:%M:%S")
            fecha_actual = datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f")
            with open(ruta_archivo, 'a') as archivo:
                archivo.write("\n\n*** {} | Inicio Lectura Facturas de Ventas ***\n".format(fecha_actual))

            #print (servicios_de_empresas_endpoints)
            #obtener_respuesta_get_services

            #inicio de lecturas...
            endpoints_por_leer = obtener_lectura_facturas_ventas(cone) #endpoint activados de lecturas. #llamada de respuesta de facturas de ventas...
           
            CANT_LECT_GENE = len(endpoints_por_leer) #cantidad de endpoints por leer.
          
             
            ID_LOG_DETA = insertar_datos_log(cone)
            
            print ("id_log",ID_LOG_DETA)

            cant_lect = 0 #filas totales
            ok_lect  = 0 #filas insertadas
            fail_lect = 0 #filas con fallas
            coment_lect = 'Ok'
            bsr = 'La busqueda no produjo ningun resultado'

            contador = 1
            # referencia a items en respuesta
            for endpoint in endpoints_por_leer:
                
                ID_LECTURA_DETA = endpoint[0]
                
                id_emp = endpoint[1]
                max_reintentos = endpoint[2]
                url = endpoint[3]
                item_por_pagina = endpoint[4]
                numero_pagina = endpoint[5]
                token = endpoint[6]

                nombre_empresa = obtener_nombre_empresa(cone, id_emp) #nombre según el id de empresa
                
                ID_ESTATUS_DETA = ENDPOINTS_OK #LECTURAS DE METODOS A LEER OBTENIDAS        
                MENSAJE_COMPLETO_GEN = ''



                for reintento in range(max_reintentos):
                    flag = None

                    CANT_REINTENTOS_DETA = reintento+1

                    FCH_INICIO_LECT_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 

                    respuesta_completa = obtener_respuesta(url, item_por_pagina, numero_pagina, token) 

                    respuesta_info_clientes = obtener_clientes_por_empresa(token)#información de clientes (comuna por ej.) a insertar...
                    
                    if respuesta_completa.status_code == 200:
                        
                        MENSAJE_COMPLETO_GEN = ''
                        #MENSAJE_COMPLETO_GEN + 'OK:' + "Empresa: "+str(nombre_empresa[0]) + " Id lectura:" + str(ID_LECTURA_DETA) + "\n"  

                        print ("Respuesta Exitosa Código 200 url: ")
                        print ("Empresa: "+str(nombre_empresa[0]) + " Id lectura: " + str(ID_LECTURA_DETA) + "\n" )

                        with open(ruta_archivo, "a") as archivo:                   
                            archivo.write( "\nRespuesta Exitosa Código 200 url: "+url+"\n")
                                                    
                        flag = True
                        break 

                    if reintento < max_reintentos - 1:
                        CANT_REINTENTOS_DETA = reintento + 1
                        # Esperar antes del próximo reintento
                        with open(ruta_archivo, "a") as archivo:                   
                            archivo.write("\nLectura fallida intento numero: "+str(reintento+1)+" | Url: "+url)
                            
                        tiempo_espera = 10  # Ejemplo: esperar 10 segundos
                        time.sleep(tiempo_espera)
                ##fin bucle intento (de reintento a max reintentos)

                                 
                    '''else: #respuesta no dio codigo 200..
                        items_totales = respuesta_completa.json()['totalItems']
                        cod_rpta = respuesta_completa.status_code
                        mensaje = respuesta_completa.json()['message']
                        #print(mensaje)

                        newMensaje = mensaje

                       # log_detalle = fn_insertar_datos_log_detalle(id_log, _id_estatus, _fch_dato, _fch_inicio_lect, _fch_fin_lect, _cant_read, _cant_insert, _conexion)
                       
                       # if mensaje == bsr: #bsr es busqueda sin resultado
                        #    contador_bsr += 1



                        print("{} | {} |  Reintento: {} | Código Respuesta: {} | Mensaje: {}\n".format(fecha_actual, nombre_empresa[0], reintento+1, cod_rpta, mensaje))
                
                        with open(ruta_archivo, 'a') as archivo:
                            archivo.write("{} | {} |  Reintento: {} | Código Respuesta: {} | Mensaje: {}\n".format(fecha_actual, nombre_empresa[0], reintento+1, cod_rpta, mensaje))
                
                        '''
                    
                items_totales = 0
                if flag:                    
                    fecha_actual = datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f")
                    #print("Leyendo Empresa:",nombre_empresa[0])
                    items_totales = 0
                   
                    if respuesta_completa.json()['totalItems'] != 0 :
                        items_totales = respuesta_completa.json()['totalItems']
                    else : items_totales = 0
                    #print ("Filas Respuesta: ", items_totales) 
                   
                    
                    #insertar clientes...

                    #información de clientes a insertar...
                    #respuesta_info_clientes = obtener_clientes_por_empresa(token)


                    clientes_insertados_actualizados = 0
                    clientes_insertados_actualizados = insertar_actualizar_datos_usuario(respuesta_info_clientes.json(), cone)
                    print ("Clientes Actualizados o Insertados: ",clientes_insertados_actualizados)
                    
                    #tabla rompimiento entre tbl_clientes_proveedores y empresas:
                    print ("item totales: ",items_totales)
                   
                    if items_totales > 0: #si hay al menos un registro, intento insertar a la tabla de rompimiento.-
                        lista_tbl_clientes_proveedores_empresas = lista_para_tbl_clientes_proveedores_empresas(respuesta_completa.json()['saleList'], id_emp, cone)
                        
                        #test de lista:
                        #print("lista tbl cli prov emp: ", str(lista_tbl_clientes_proveedores_empresas))
                        #print("\n")
                        
                        filas_insertadas_tbl_cli_prov_emp = insert_lista_para_tbl_clientes_proveedores_empresas(lista_tbl_clientes_proveedores_empresas, cone)
                        
                        if filas_insertadas_tbl_cli_prov_emp > 1 :
                            print("Nuevos Clientes en tbl_clientes_proveedores_empresas: "+str(filas_insertadas_tbl_cli_prov_emp))
                            with open(ruta_archivo, "a") as archivo:                   
                                archivo.write( "Nuevos Clientes en tbl_clientes_proveedores_empresas: "+str(filas_insertadas_tbl_cli_prov_emp) )                      
                    
                    ###fin tabla de rompimiento



                    #metodo 01/08 sin token:
                    #facturas_insertadas = insertar_saleList(respuesta_completa.json(), cone, id_emp)  
                    #envío el token, porque lo ocupo para el pdf y xml..
                   
                   
                    #INSERTAR DATOS DE FACTURAS
                    facturas_insertadas = insertar_saleList(respuesta_completa.json(), cone, id_emp, token)                  
                    
                    #test de retornar un 2 si es que ya existe la factura o el detalle....
                    #facturas_insertadas = insertar_saleList_2(respuesta_completa.json(), cone, id_emp, token)             
                   
                   
                   
                    cod_rpta = respuesta_completa.status_code
                    mensaje = respuesta_completa.json()['message']

                    #suma de totales de log registro..
                    #CANT_READ_DETA += items_totales
                    #CANT_INSERT_DETA += facturas_insertadas #cantidad de facturas insertadas
                    #fail_lect = 0

                    ##aqui inserto el detalle del log.
                    #detalle_log = fn_insertar_datos_log_detalle(id_log, _id_estatus, _fch_dato, _fch_inicio_lect, _fch_fin_lect, _cant_read, _cant_insert, _conexion)
                    
                    with open (ruta_archivo, 'a') as archivo:
                        archivo.write("{} | {} | Filas Insertadas: {} | Filas Respuesta: {}  | Código Respuesta {} | {} \n".format(fecha_actual,nombre_empresa[0],facturas_insertadas,items_totales, cod_rpta, mensaje) )   
                    contador += 1
                    print("{} | {} | Filas Insertadas: {} | Filas Respuesta: {}  | Código Respuesta {} | {} \n".format(fecha_actual,nombre_empresa[0],facturas_insertadas,items_totales, cod_rpta, mensaje) )   
                

                    CANT_READ_DETA = items_totales
                    CANT_INSERT_DETA = facturas_insertadas
                    FCH_DATO_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 
                    FCH_FIN_LECT_DETA = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                    #ID_ESTATUS_DETA = RESPUESTA_OK 
                    ID_ESTATUS_DETA = FIN_OK
                    
                    print ("CANT_READ_DETA ", str(items_totales))
                    print ("CANT_INSERT_DETA ", str(facturas_insertadas))


                    #INSERCIÓN EN LOG DE DETALLE:
                    '''print ("id log det: "+str(ID_LOG_DETA))
                    print ("id estatus det: "+str(ID_ESTATUS_DETA))
                    print ("FCH_DATO_DETA: "+str(FCH_DATO_DETA))

                    print ("FCH_INICIO_LECT_DETA: "+str(FCH_INICIO_LECT_DETA))
                    print ("FCH_FIN_LECT_DETA: "+str(FCH_FIN_LECT_DETA))
                    print ("CANT_READ_DETA: "+str(CANT_READ_DETA))
                    print ("CANT_INSERT_DETA: "+str(CANT_INSERT_DETA))
                    print ("ID_LECTURA_DETA: "+str(ID_LECTURA_DETA))
                    print ("CANT_REINTENTOS_DETA: "+str(CANT_REINTENTOS_DETA))'''
                    

                    insert_log_detalle_ok = insertar_datos_log_detalle_defontana(cone, ID_LOG_DETA, ID_ESTATUS_DETA, FCH_DATO_DETA, FCH_INICIO_LECT_DETA, FCH_FIN_LECT_DETA, CANT_READ_DETA, CANT_INSERT_DETA, ID_LECTURA_DETA, CANT_REINTENTOS_DETA )
                    print ("insert log det ok: ", str(insert_log_detalle_ok))
                    OK_LECT_GENE += 1
                    print ("*****")


                else: #respuesta no fue status code 200
                    
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
                    #ID_ESTATUS_DETA = RESPUESTA_ALERT #al menos una respuesta entró en error.

                    #INSERCIÓN BD tabla tbl_log_detalle_sii
                    insert_log_det_fail = insertar_datos_log_detalle_defontana(cone, ID_LOG_DETA, ID_ESTATUS_DETA, FCH_DATO_DETA, FCH_INICIO_LECT_DETA, FCH_FIN_LECT_DETA, CANT_READ_DETA, CANT_INSERT_DETA, ID_LECTURA_DETA, CANT_REINTENTOS_DETA)
                    print ("insert log det fail: ",str(insert_log_det_fail))
                    FAIL_LECT_GENE += 1


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
              

            '''print("*****")
            print(id_log)
            print(fch_cada_10min)
            print(fecha_fin)
            print(cant_lect)
            print(ok_lect)
            print(fail_lect)
            print(coment_lect)'''

            #fn_insertar_datos_log_actualiza(_id_log, _id_servicio, _fch_inicio_lect, _fch_fin_lect, _cant_lect, _ok_lect , _fail_lect, _coment_lect,  _conexion)
            #idla = fn_insertar_datos_log_actualiza(id_log, 3, fch_cada_10min, fecha_fin, cant_lect, ok_lect , fail_lect, coment_lect,  cone)

           # print ("log actualizado ",idla)

            cant_lect = 0 #filas totales
            ok_lect  = 0 #filas insertadas
            fail_lect = 0 #filas con fallas


            '''print (cant_lect)
            print (ok_lect)
            print ( fail_lect)'''

            with open(ruta_archivo, 'a') as archivo:
                archivo.write("*** FIN LECTURA FACTURAS VENTAS: {} ***\n\n".format(fecha_actual) )   
            cone.commit()
            cone.close()       
        
        print("fin main")       
    except Exception as e:
        import traceback
        error_msg = traceback.format_exc()
        print("Error en obtener_facturas_ventas:\n", error_msg)       
        # Registra el error en el log del servicio
        with open(ruta_archivo, "a") as log_file:
            log_file.write("Error en obtener_facturas_ventas:\n")
            log_file.write(str(datetime.now()) + " | " +error_msg + "\n")

if __name__ == "__main__":
    main()

