
import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import os
import sys
import time
import obtener_facturas_ventas
from datetime import datetime, timedelta


class MyService(win32serviceutil.ServiceFramework):
    # nombre del servicio
    _svc_name_ = "APIServiceDeFontana"
    # nombre del servicio que se muestra
    _svc_display_name_ = "APIServiceDeFontana"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        #23-06..:
        self.stop_requested = False

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        #23-06..:
        self.stop_requested = True

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def main(self):
        while True:
            if self.stop_requested:
                break

            # Ejecuta el método run_script_at_intervals con un intervalo de 10 minutos
            self.run_script_at_intervals(120) #120 es 2 horas.

            if self.stop_requested:
                break
    
    def execute_script(self, execution_time):
        # Aquí colocas el código que deseas ejecutar
        print(f"Ejecutando el script en {execution_time}...")
        obtener_facturas_ventas.main()
        

    def run_script_at_intervals(self, interval_minutes):
        while True:
            now = datetime.now()
            next_execution = now + timedelta(minutes=interval_minutes - now.minute % interval_minutes, seconds=-now.second)
            
            print(f"Esperando hasta las {next_execution.strftime('%d-%m-%Y %H:%M:%S')} para ejecutar el script...")
            
            time_to_wait = (next_execution - now).total_seconds()
            
            if time_to_wait > 0:
                time.sleep(time_to_wait)
                self.execute_script(next_execution.strftime("%d-%m-%Y %H:%M:%S"))
            else:
                self.execute_script(next_execution.strftime("%d-%m-%Y %H:%M:%S"))
                time.sleep((interval_minutes * 60) - now.second)

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(MyService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(MyService)