
import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import os
import sys
import time
import obtener_facturas_ventas


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

    '''
    def main(self):
        #while True: mejorado el día 23-06:
        while not self.stop_requested:

            detalle_compras.main()
            detalle_ventas.main()
            
            time.sleep(300) #120segundos es cada 2 minutos. #300segundos es cada 5 minutos, #1800seg es media hora #600 seg 10 minutos.
    '''
    #27-06 nuevo main.
    def main(self):
        while True:
            if self.stop_requested:
                break
            #120segundos es cada 2 minutos. #300segundos es cada 5 minutos, #1800seg es media hora #600 seg 10 minutos.
            
            #detalle_compras.main()
            obtener_facturas_ventas.main()
            time.sleep(300)  # Espera de N segundos antes de ejecutar el siguiente main

            if self.stop_requested:
                break           
          
               
            # Espera de 5 minutos (300 segundos) antes de volver a ejecutar ambos main
            for _ in range(30):
                if self.stop_requested:
                    break
                time.sleep(10)  # Espera de 10 segundos antes de verificar nuevamente si se ha solicitado detener el servicio
    #27-06 nuevo main..

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(MyService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(MyService)