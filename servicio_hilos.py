import win32serviceutil
import win32service
import win32event
import servicemanager
import sys
import time
import socket
from datetime import datetime, timedelta

import lecturas_grupos  

class ApiServiceSii_hilos(win32serviceutil.ServiceFramework):
    _svc_name_ = 'ApiServiceDefontana_Hilos'
    _svc_display_name_ = 'ApiServiceDefontana_Hilos'
    
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.stop_requested = False
    
    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.stop_requested = True
    
    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def main(self):
        while not self.stop_requested:
            self.run_script_at_intervals(60)

    def execute_script(self):
        print(f"Ejecutando el script...")
        lecturas_grupos.main()

    def run_script_at_intervals(self, interval_minutes):
        primera_ejecucion = True

        while not self.stop_requested:
            now = datetime.now()

            if primera_ejecucion:
                # Para la primera ejecución, calcula el tiempo hasta el próximo minuto exacto
                next_execution = now + timedelta(seconds=60 - now.second)
                primera_ejecucion = False
            else:
                next_execution = now + timedelta(minutes=interval_minutes - now.minute % interval_minutes, seconds=-now.second)

            print(f"Esperando hasta las {next_execution.strftime('%d-%m-%Y %H:%M:%S')} para ejecutar el script...")

            time_to_wait = (next_execution - now).total_seconds()

            # Dividir el tiempo de espera en segmentos de 1 segundo
            for _ in range(int(time_to_wait)):
                if self.stop_requested:
                    return
                time.sleep(1)

            self.execute_script()

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(ApiServiceSii_hilos)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(ApiServiceSii_hilos)
