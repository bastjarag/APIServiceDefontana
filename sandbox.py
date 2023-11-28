from datetime import datetime, timedelta

# VARIABLES PARA LOG GENERAL
ID_SERVICIO_GENE = 3 #3 ES DEFONTANA

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

