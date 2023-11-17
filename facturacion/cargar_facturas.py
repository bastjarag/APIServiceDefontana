import requests
import json
import time
from datetime import datetime

# Función para enviar la factura
def enviar_factura(json_factura, headers):
    url = "https://replapi.defontana.com/api/Sale/Savesale"
    response = requests.post(url, headers=headers, data=json_factura)
    return response

# Función para leer el archivo y enviar las facturas
def leer_archivo_y_enviar_facturas(ruta_archivo):
    with open(ruta_archivo, 'r') as file:
        contenido = file.read()

    facturas = contenido.split('--- Fin Factura ')
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJuYW1laWQiOiJBRDEyM0ZULUhHREY1Ni1LSTIzS0wtS0pUUDk4NzYtSEdUMTIiLCJ1bmlxdWVfbmFtZSI6ImNsaWVudC5sZWdhY3lAZGVmb250YW5hLmNvbSIsImh0dHA6Ly9zY2hlbWFzLm1pY3Jvc29mdC5jb20vYWNjZXNzY29udHJvbHNlcnZpY2UvMjAxMC8wNy9jbGFpbXMvaWRlbnRpdHlwcm92aWRlciI6IkFTUC5ORVQgSWRlbnRpdHkiLCJBc3BOZXQuSWRlbnRpdHkuU2VjdXJpdHlTdGFtcCI6IkdIVEQyMzQtS0xISjc4NjgtRkc0OTIzLUhKRzA4RlQ1NiIsImNvbXBhbnkiOiIyMDIxMDgxNjE1NTQzMTcwNTUzMyIsImNsaWVudCI6IjIwMjAwODI2MTkwMTIxODU0MDAyIiwib2xkc2VydmljZSI6InB5bWUiLCJ1c2VyIjoiSVQiLCJzZXNzaW9uIjoiMTY5ODY4MzYxMSIsInNlcnZpY2UiOiJweW1lIiwiY291bnRyeSI6IkNMIiwiY29tcGFueV9uYW1lIjoiUEZWIEVMIFRJVVFVRSBTUEEiLCJjb21wYW55X2NvdW50cnkiOiJDaGlsZSIsInVzZXJfbmFtZSI6IklUIiwicm9sZXNQb3MiOiJbXCJ1c3VhcmlvXCIsXCJ1c3VhcmlvZXJwXCJdIiwicnV0X3VzdWFyaW8iOiIyNS4zNjAuNDU1LTciLCJpc3MiOiJodHRwczovLyouZGVmb250YW5hLmNvbSIsImF1ZCI6IjA5OTE1M2MyNjI1MTQ5YmM4ZWNiM2U4NWUwM2YwMDIyIiwiZXhwIjoyMDc3Mjg4NDExLCJuYmYiOjE2OTg2ODM2MTF9.daLOIWMyLSO_qOeZkucGUhi2D7TesMnP-FQdBHjfsBk'
    }
    
    # Abrimos el log file una sola vez para optimizar
    with open("log_facturas.txt", "a") as log_file:
        # Registrar la hora de inicio
        log_file.write(f"Inicio del proceso: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        print(f"Inicio del proceso: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        for factura in facturas:
            if factura.strip() == "":
                continue  # Saltar si es una cadena vacía

            inicio_json = factura.find('{')
            fin_json = factura.rfind('}')
            if inicio_json == -1 or fin_json == -1:
                continue  # Saltar si no hay JSON

            json_factura = factura[inicio_json:fin_json+1]

            try:
                response = enviar_factura(json_factura, headers)
                
                ''' # Imprimir y guardar la respuesta
                respuesta_texto = f"Respuesta: {response.status_code} {response.text}\n"
                log_file.write(respuesta_texto)
                print(respuesta_texto)'''
                
                # Get the current time
                current_time = datetime.now()

                # Format the date and time as dd mm yyyy hh mm ss sss
                tiempo_formateado = current_time.strftime("%d %m %Y %H %M %S %f")[:-3]

                # Imprimir y guardar el JSON de la respuesta
                respuesta_json = f"{tiempo_formateado} | Respuesta JSON: {response.json()}\n"
                log_file.write(respuesta_json)
                print(respuesta_json)
                
                log_file.write("----\n")
                print("----")

                # Esperar 200 milisegundos antes de enviar la siguiente factura
                time.sleep(0.2)

            except Exception as e:
                error_msg = f"Error al enviar factura: {e}\n"
                log_file.write(error_msg)
                print(error_msg)

                factura_erronea = f"Factura errónea: {json_factura}\n"
                log_file.write(factura_erronea)
                print(factura_erronea)
                
                log_file.write("----\n")
                print("----\n")


def main():
    ruta_archivo = "C:/Users/OE/Desktop/facturacion_test_defontana_1000_facturas.txt"
    leer_archivo_y_enviar_facturas(ruta_archivo)


if __name__ == "__main__":
    main()
