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

def obtener_respuesta_compra(_url, _item_por_pagina, _numero_pagina, _token, resta_mes_periodo):
    fecha_actual = datetime.now()

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
    #print ("response json obtener respuesta compra")
    #print(response.json())
    return response

def formatear_rut(rut):
    
    rut_formateado = ''.join(filter(str.isdigit, rut))

    # Si el rut formateado tiene más de 1 caracter, eliminar el último que es el dígito verificador
    if len(rut_formateado) > 1:
        rut_formateado = rut_formateado[:-1]

    return rut_formateado

#pasar a funcion de bd
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
    

def lista_para_tbl_clientes_proveedores_empresas_compras(json_data, id_emp, _conexion):
    id_tipo_cliprov = 2  #en BD: tbl_tipos_clientes_proveedores: 1 = Cliente, 2 = Proveedor
    
    lista_cli_prov_emp = []

    for data_item in json_data:
        rut = data_item.get("providerId")  # Obtener el rut del elemento JSON (ESTÁ CON PUNTOS Y GUION)
        
        if rut:
            rut_formateado = formatear_rut(rut) #pasa de rut con puntos guion y dv. A: sin puntos y sin dv.
            id_cliprov = obtener_id_cliprov(rut_formateado, _conexion)

            if id_cliprov is not None:
                # Agregar una tupla con los valores a la lista
                lista_cli_prov_emp.append((id_cliprov, id_emp, id_tipo_cliprov))
    
    return lista_cli_prov_emp


###revisar que es el mismo para compras y ventas segun la lista entregada como keys
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


def insertar_purchaseList(_respuesta, _conexion, _id_emp, _token):
   
    purchaseList = _respuesta #dentro del json 

    # Comprobar si 'data' está en purchaseList y si 'data' está dentro de ese 'data'
    if 'data' in purchaseList and 'data' in purchaseList['data']:
        purchaseData = purchaseList['data']['data']  # Lista que quieres iterar
    else:
        print("La estructura del JSON no es la esperada.")
        return 0  # O manejar de otra manera
    
    '''# Verificamos si la respuesta contiene 'data' y luego el segundo 'data' que es una lista
    if 'data' in _respuesta and 'data' in _respuesta['data']:
        purchaseList = _respuesta['data']['data']  # Accedemos al nivel correcto de 'data'
        print("purchaseList")
        print(purchaseList)
    else:
        print("La estructura del JSON no es la esperada.")
        return 0  # O manejar de otra manera'''
    
    '''# Ya tienes la lista de compras directamente, no necesitas buscar 'data' nuevamente
    print("purchaseList")
    print(purchaseList)'''

    # Imprimir para depurar
    print("Contenido de purchaseList['data']: ", purchaseList['data'])

    filas_afectadas_total = 0 #facturas insertadas.
    
    '''for purchase in purchaseList['data']:
        #mapeo de campos
        origin = purchase['origin']
        companyId = purchase['companyId']
        providerLegalCode = purchase['providerLegalCode']
        providerName = purchase['providerName']
        documentNumber = purchase['documentNumber']
        documentType = purchase['documentType']
        documentTotal = purchase['documentTotal']
        documentEmissionDate = purchase['documentEmissionDate']
        documentEntryType = purchase['documentEntryType']
        documentPlatformId = purchase['documentPlatformId']
        siiReceiptDate = purchase['siiReceiptDate']
        lastStatus = purchase['lastStatus']
        isIntegrated = purchase['isIntegrated']
        isDigital = purchase['isDigital']
        isReceived = purchase['isReceived']
        providerId = purchase['providerId']
        siiDocumentType = purchase['siiDocumentType']
        documentTypeId = purchase['documentTypeId']
        fiscalYear = purchase['fiscalYear']
        voucherTypeId = purchase['voucherTypeId']
        voucherNumber = purchase['voucherNumber']
        accoutingBalance = purchase['accoutingBalance']
        key = purchase['key']
        canEdit = purchase['canEdit']
        canRemove = purchase['canRemove']
        deleteFailCondition = purchase['deleteFailCondition']
        canFix = purchase['canFix']
        canCancel = purchase['canCancel']
        canReceive = purchase['canReceive']
        canReject = purchase['canReject']
        canLey19983 = purchase['canLey19983']
        canGenericView = purchase['canGenericView']
        canPreview = purchase['canPreview']
        canPrint = purchase['canPrint']
        canReceipt = purchase['canReceipt']
        hasStockDocumentsRelated = purchase['hasStockDocumentsRelated']
        hasMenu = purchase['hasMenu']
        originDescription = purchase['originDescription']'''
      
        
        # buscar rut cliente en tbl_clientes_proveedores
    for purchase in purchaseData:
        # Mapeo de campos con .get() para evitar KeyError si una clave no existe
        origin = purchase.get('origin', 'Valor por defecto si no existe')
        companyId = purchase.get('companyId', 'Valor por defecto si no existe')
        providerLegalCode = purchase.get('providerLegalCode', 'Valor por defecto si no existe')
        providerName = purchase.get('providerName', 'Valor por defecto si no existe')
        documentNumber = purchase.get('documentNumber', 'Valor por defecto si no existe')
        documentType = purchase.get('documentType', 'Valor por defecto si no existe')
        documentTotal = purchase.get('documentTotal', 'Valor por defecto si no existe')
        documentEmissionDate = purchase.get('documentEmissionDate', 'Valor por defecto si no existe')
        documentEntryType = purchase.get('documentEntryType', 'Valor por defecto si no existe')
        documentPlatformId = purchase.get('documentPlatformId', 'Valor por defecto si no existe')
        siiReceiptDate = purchase.get('siiReceiptDate', 'Valor por defecto si no existe')
        lastStatus = purchase.get('lastStatus', 'Valor por defecto si no existe')
        isIntegrated = purchase.get('isIntegrated', 'Valor por defecto si no existe')
        isDigital = purchase.get('isDigital', 'Valor por defecto si no existe')
        isReceived = purchase.get('isReceived', 'Valor por defecto si no existe')
        providerId = purchase.get('providerId', 'Valor por defecto si no existe')
        siiDocumentType = purchase.get('siiDocumentType', 'Valor por defecto si no existe')
        documentTypeId = purchase.get('documentTypeId', 'Valor por defecto si no existe')
        fiscalYear = purchase.get('fiscalYear', 'Valor por defecto si no existe')
        voucherTypeId = purchase.get('voucherTypeId', 'Valor por defecto si no existe')
        voucherNumber = purchase.get('voucherNumber', 'Valor por defecto si no existe')
        accoutingBalance = purchase.get('accoutingBalance', 'Valor por defecto si no existe')
        key = purchase.get('key', 'Valor por defecto si no existe')
        canEdit = purchase.get('canEdit', 'Valor por defecto si no existe')
        canRemove = purchase.get('canRemove', 'Valor por defecto si no existe')
        deleteFailCondition = purchase.get('deleteFailCondition', 'Valor por defecto si no existe')
        canFix = purchase.get('canFix', 'Valor por defecto si no existe')
        canCancel = purchase.get('canCancel', 'Valor por defecto si no existe')
        canReceive = purchase.get('canReceive', 'Valor por defecto si no existe')
        canReject = purchase.get('canReject', 'Valor por defecto si no existe')
        canLey19983 = purchase.get('canLey19983', 'Valor por defecto si no existe')
        canGenericView = purchase.get('canGenericView', 'Valor por defecto si no existe')
        canPreview = purchase.get('canPreview', 'Valor por defecto si no existe')
        canPrint = purchase.get('canPrint', 'Valor por defecto si no existe')
        canReceipt = purchase.get('canReceipt', 'Valor por defecto si no existe')
        hasStockDocumentsRelated = purchase.get('hasStockDocumentsRelated', 'Valor por defecto si no existe')
        hasMenu = purchase.get('hasMenu', 'Valor por defecto si no existe')
        originDescription = purchase.get('originDescription', 'Valor por defecto si no existe')
   

        rut_sin_dv = providerLegalCode[:-1].replace(".","").replace("-","") # quitar . - dv (punto guion y dv)
        dv = providerLegalCode[-1]
        id_proveedor = None
        
        #buscar e insertar cliente proveedor en la tabla d ela bd...
        try:
            cursor = _conexion.cursor()
            #cursor.callproc('fnc_defontana_bus_ins_cliprov', [
            cursor.callproc('fn_defontana_buscar_insertar_cliprov', [   
                                                    providerLegalCode, 
                                                    rut_sin_dv,
                                                    dv
                                            
                                                    ])
            id_proveedor = cursor.fetchone()
            id_proveedor = id_proveedor[0] # Obtener el id cliente
           
            if id_proveedor == 0: 
                _conexion.rollback()
             
        except Exception as e:
            _conexion.rollback()
            print("ERROR: {}".format(e))
            with open (ruta_archivo, 'a') as archivo:
                archivo.write("\nError al obtener ID de Cliente Proveedor desde la base de datos...")
            
    
        if id_proveedor > 0:
            try:
            # print("dentro de factrura vendta....")
                fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cursor = _conexion.cursor()
                #cursor.callproc('fnc_defontana_ins_ventas_b', [

                cursor.callproc('fn_defontana_insertar_factura_compra', [                                                            
                                                        _id_emp,
                                                        id_proveedor,
                                                        fecha_actual,
                                                        #### 1am 23-1-24
                                                        origin,
                                                        companyId,
                                                        providerLegalCode,
                                                        providerName,
                                                        documentNumber,
                                                        documentType,
                                                        documentTotal,
                                                        documentEmissionDate,
                                                        documentEntryType,
                                                        documentPlatformId,
                                                        siiReceiptDate,
                                                        lastStatus,
                                                        isIntegrated,
                                                        isDigital,
                                                        isReceived,
                                                        providerId,
                                                        siiDocumentType,
                                                        documentTypeId,
                                                        fiscalYear,
                                                        voucherTypeId,
                                                        voucherNumber,
                                                        accoutingBalance,
                                                        key,
                                                        canEdit,
                                                        canRemove,
                                                        deleteFailCondition,
                                                        canFix,
                                                        canCancel,
                                                        canReceive,
                                                        canReject,
                                                        canLey19983,
                                                        canGenericView,
                                                        canPreview,
                                                        canPrint,
                                                        canReceipt,
                                                        hasStockDocumentsRelated,
                                                        hasMenu,
                                                        originDescription,
                                                        #### 1am 23-1-24
                                                        
                                                        ])
                
                resultado = cursor.fetchone()# Recupera el valor de retorno de la función.                
                valor_retorno = resultado[0]
                
                #probar que inserte facturas...
                print("*"*20)
                print("factura: ",documentNumber)
                print("insertó factura: ",valor_retorno)
                
                if valor_retorno == 0:
                    _conexion.rollback()

                if valor_retorno == 1:
                    filas_afectadas_total += valor_retorno 

                '''if valor_retorno == 2: #factura ya existe en bd, sumar a la variable que me cuenta las facturas en bd.
                    f_e_bd = f_e_bd +1 
                 '''       
                    
                #_conexion.commit()   #forzar la insercion
                              
            except Exception as e:
                _conexion.rollback()
                print("ERROR: {}".format(e))
                with open (ruta_archivo, 'a') as archivo:
                    archivo.write("\nError al insertar factura en la base de datos..")
                filas_afectadas_total = -1
            #esto no lo tiene actualmente las facturas de compra
            # details = purchaseList['details'] #items de cada factura...

            #valor_retorno_detalle = 0 #para retornar el valor de detalles insertados, consultado despues...
           
            # Insertar detalles de ventas
            '''if valor_retorno > 0: #si la factura se insertó, me insertará detalle...
                
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
                if valor_retorno_detalle > 0: 
                     
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
'''
     #factura insertada en bd..      
    
    print ("filas afectadas total:")
    print (filas_afectadas_total)
    return filas_afectadas_total


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

def procesar_factura_compra(lectura, ruta_archivo, ID_LOG, hora_orquestado):
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

    cone = conexion_base_datos()#connection_database() 
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

            respuesta_inicial = obtener_respuesta_compra(url, item_por_pagina, numero_pagina, token, resta_mes_periodo)
            

            
     
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

                    #esto es en ventas, que se llama totalItems.
                    #total_items = respuesta_inicial.json()['totalItems']
                    # Verificamos si la respuesta contiene 'data' y luego 'recordsFiltered'
                    respuesta_json = respuesta_inicial.json()
                    if 'data' in respuesta_json and 'recordsFiltered' in respuesta_json['data']:
                        total_items = respuesta_json['data']['recordsFiltered']
                    else:
                        print("La clave 'recordsFiltered' no se encuentra en la respuesta JSON")
                        total_items = 0


                    total_paginas = calcular_total_paginas(total_items, int(item_por_pagina))
                    
                    print ("total items recibido "+ str(total_items))
                    print ("total paginas a recorrer "+ str(total_paginas))

                    # Procesa cada página
                    for numero_pagina_actual in range(1, total_paginas + 1):
                        #print ("entre a procesar paginas")
                        print ("num pag actual: " + str(numero_pagina_actual))

                        #respuesta_completa = obtener_respuesta(url, item_por_pagina, str(numero_pagina_actual), token)
                        respuesta_completa = obtener_respuesta_compra(url, item_por_pagina, str(numero_pagina_actual), token, resta_mes_periodo)


                        # Aquí comienza la lógica de procesamiento de respuesta_completa
                        if respuesta_completa.status_code == 200:
                           
                            
                            # Accediendo a 'data' y luego a 'recordsTotal' dentro de la respuesta JSON
                            items_totales = total_items#respuesta_completa.json()['data']['recordsTotal']

                            total_filas_leidas += items_totales  # Acumula el total de filas leídas


                            if items_totales > 0: #si hay al menos un registro, intento insertar a la tabla de rompimiento.-

                                #lista_tbl_clientes_proveedores_empresas_compras = lista_para_tbl_clientes_proveedores_empresas_compras(respuesta_completa.json()['saleList'], id_emp, cone)
                                # Verificamos si la respuesta contiene 'data' y luego el segundo 'data' que es una lista
                                if 'data' in respuesta_json and 'data' in respuesta_json['data']:
                                    lista_datos = respuesta_json['data']['data']  # Esta es la lista que contiene los datos

                                    lista_tbl_clientes_proveedores_empresas_compras = lista_para_tbl_clientes_proveedores_empresas_compras(lista_datos, id_emp, cone)
                                else:
                                    print("La clave 'data' no se encuentra en la respuesta JSON o su estructura interna es diferente")
                                    # Manejar el caso de que la estructura no sea la esperada
                                
                                filas_insertadas_tbl_cli_prov_emp = insert_lista_para_tbl_clientes_proveedores_empresas(lista_tbl_clientes_proveedores_empresas_compras, cone)
                        
                                if filas_insertadas_tbl_cli_prov_emp > 1 :
                                    print("Nuevos Clientes en tbl_clientes_proveedores_empresas: "+str(filas_insertadas_tbl_cli_prov_emp))
                                    with open(ruta_archivo, "a") as archivo:                   
                                        archivo.write( "\nNuevos Clientes en tbl_clientes_proveedores_empresas: "+str(filas_insertadas_tbl_cli_prov_emp+ "\n") )                      
                            

                                #INSERTAR DATOS DE FACTURAS
                                facturas_insertadas = insertar_purchaseList(respuesta_json, cone, id_emp, token)   
                                
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
                archivo.write("*** FIN LECTURA FACTURAS COMPRAS: {} ***\n\n".format(fecha_actual) )   
            cone.commit()
            cone.close()                

          
            # ...
            
            #print("fin main") 




        except Exception as e:
            import traceback
            error_msg = traceback.format_exc()
            print("Error:\n", error_msg)
  

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
       
            '''if id_tipo_lectura == 5: 
                procesar_factura_venta(lectura, ruta_archivo, ID_LOG, called_execution_time)
            
            elif id_tipo_lectura == 6:
                procesar_caf(lectura, ruta_archivo, ID_LOG, called_execution_time)
               ''' 
            
            #el
            if id_tipo_lectura == 10:
                #entre al tipo de lectura 10, es de compras
                print("ID TIPO LECTURA 10 COMPRAS DEFONTANA")
                procesar_factura_compra(lectura, ruta_archivo, ID_LOG, called_execution_time)
            
            

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
        conexion = conexion_base_datos() #connection_database()

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

def cargar_tipos_lectura(id_serv):
    tipos_lectura = {}
    # Conectar con la base de datos y ejecutar una consulta 
    # para obtener los id_tipo_lect y su correspondiente nombre 
    # para el id_serv especificado.

    conexion = conexion_base_datos() #connection_database()  # Conexión a la base de datos
    cursor = conexion.cursor()
    query = "SELECT id_tipo_lect, nom_lect FROM tbl_tipos_lecturas WHERE id_serv = %s"
    cursor.execute(query, (id_serv,))
    
    for id_tipo_lect, nom_lect in cursor.fetchall():
        tipos_lectura[id_tipo_lect] = nom_lect

    cursor.close()
    conexion.close()

    return tipos_lectura




# Diccionario global para rastrear hilos activos por grupo
active_threads = {} 

def main():
    conexion = conexion_base_datos() #Econnection_database()
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


