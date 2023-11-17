import psycopg2
from datetime import datetime
import requests

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

def post_exchange_rate(token, coin_id, date, rate):
    url = "https://replapi.defontana.com/api/Sale/SaveExchangeRate"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    data = {
        "coinID": coin_id,
        "date": date,
        "rate": rate
    }
    response = requests.post(url, headers=headers, json=data)
    return response.json()

def get_usd_values(start_date, end_date):
    conn = connection_database()
    if conn is None:
        print("No se pudo establecer una conexión con la base de datos.")
        return []

    cursor = conn.cursor()
    try:
        '''# Ejecutar una consulta simple para probar el cursor y la conexión
        cursor.execute("SELECT 1;")
        test_value = cursor.fetchone()
        print("Prueba de conexión y cursor exitosa, resultado:", test_value)

        # Ahora intentamos con la consulta real, pero simplificada y sin parámetros
        print("Ejecutando consulta simplificada sin parámetros.")
        cursor.execute("SELECT fch_ind, valor_ind FROM tbl_datos_indicadores WHERE id_ind = 1;")
        test_values = cursor.fetchall()
        print("Resultados de la consulta simplificada:", test_values)'''

        # Si todo va bien, procedemos con la consulta original y los parámetros
        query = """
        SELECT DISTINCT fch_ind, valor_ind
        FROM tbl_datos_indicadores
        
        WHERE id_ind = 1
         AND fch_ind BETWEEN %s AND %s
        ORDER BY fch_ind DESC
        """
        #print(f"Ejecutando consulta original con fechas: {start_date}, {end_date}")
        cursor.execute(query, (start_date, end_date))
        values = cursor.fetchall()
        #print("Valores obtenidos de la consulta original:", values)
        return values
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
        return []
    finally:
        cursor.close()
        conn.close()
    


def main():
    start_date = '2023-11-01'
    end_date = '2023-11-03'
    token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJuYW1laWQiOiJBRDEyM0ZULUhHREY1Ni1LSTIzS0wtS0pUUDk4NzYtSEdUMTIiLCJ1bmlxdWVfbmFtZSI6ImNsaWVudC5sZWdhY3lAZGVmb250YW5hLmNvbSIsImh0dHA6Ly9zY2hlbWFzLm1pY3Jvc29mdC5jb20vYWNjZXNzY29udHJvbHNlcnZpY2UvMjAxMC8wNy9jbGFpbXMvaWRlbnRpdHlwcm92aWRlciI6IkFTUC5ORVQgSWRlbnRpdHkiLCJBc3BOZXQuSWRlbnRpdHkuU2VjdXJpdHlTdGFtcCI6IkdIVEQyMzQtS0xISjc4NjgtRkc0OTIzLUhKRzA4RlQ1NiIsImNvbXBhbnkiOiIyMDIxMDgxNjE1NTQzMTcwNTUzMyIsImNsaWVudCI6IjIwMjAwODI2MTkwMTIxODU0MDAyIiwib2xkc2VydmljZSI6InB5bWUiLCJ1c2VyIjoiSVQiLCJzZXNzaW9uIjoiMTY5ODY4MzYxMSIsInNlcnZpY2UiOiJweW1lIiwiY291bnRyeSI6IkNMIiwiY29tcGFueV9uYW1lIjoiUEZWIEVMIFRJVVFVRSBTUEEiLCJjb21wYW55X2NvdW50cnkiOiJDaGlsZSIsInVzZXJfbmFtZSI6IklUIiwicm9sZXNQb3MiOiJbXCJ1c3VhcmlvXCIsXCJ1c3VhcmlvZXJwXCJdIiwicnV0X3VzdWFyaW8iOiIyNS4zNjAuNDU1LTciLCJpc3MiOiJodHRwczovLyouZGVmb250YW5hLmNvbSIsImF1ZCI6IjA5OTE1M2MyNjI1MTQ5YmM4ZWNiM2U4NWUwM2YwMDIyIiwiZXhwIjoyMDc3Mjg4NDExLCJuYmYiOjE2OTg2ODM2MTF9.daLOIWMyLSO_qOeZkucGUhi2D7TesMnP-FQdBHjfsBk"  # Reemplazar con tu token real

    usd_values = get_usd_values(start_date, end_date)
    print("Fechas obtenidas para procesar:", [date for date, rate in usd_values])

    for date, rate in usd_values:
        iso_date = date.isoformat() + "T00:00:00.000Z"  # Ajustar el formato de fecha si es necesario
        response = post_exchange_rate(token, "USD", iso_date, rate)


        print("fecha: "+ str(date))
       
        print("res entera:" +str(response))
        

if __name__ == "__main__":
    main()
