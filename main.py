import time
import logging
from automation_process import AutomationProcess
from logger_config import configuracion_logging

if __name__ == "__main__":
    configuracion_logging()

    tiempo_inicio = time.perf_counter()

    procesador = AutomationProcess()
    resultados = procesador.ejecutar()

    tiempo_final = time.perf_counter()
    tiempo_transcurrido = tiempo_final - tiempo_inicio

    logging.info(f"Proceso finalizado.")
    logging.info(f"Archivos TAR procesados: {resultados['num_tar']}")
    logging.info(f"Documentos guardados: {resultados['num_dict']}")

    minutos, segundos = divmod(tiempo_transcurrido, 60)
    horas, minutos = divmod(minutos, 60)
    logging.info(
        f"Tiempo total de ejecucion: {int(horas):02d}h {int(minutos):02d}m {segundos:.4f}s"
    )
