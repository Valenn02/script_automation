from typing import Dict, List, Any
from pymongo import MongoClient
from pymongo.errors import PyMongoError, OperationFailure, BulkWriteError
from datetime import datetime
import logging
import time

class MongoLoggingDBHandler(logging.Handler):
    """
    Clase handler personalizada para enviar logs a MongoDB.

    Attributes:
        client (MongoClient): Cliente de conexion a conexion a MongoDB.
        db_name (str): Nombre de la base de datos en MongoDB.
        collection_logs (str): Nombre de la coleccion para guardar logs.
        buffer (List[Dict[str, Any]]): Buffer para almacenar logs.
        buffer_size (int): Tamanio del buffer (default=40).
        waiting_time (float): Tiempo en segundos de espera entre intentos (default=1).
        max_attempts (int): Numero de veces que se intenta guardar un buffer de logs en caso de error (default=3).
    """

    def __init__(self,
                mongo_uri: str,
                db_name: str,
                collection_logs: str,
                buffer_size: int=40,
                waiting_time: float=1.0,
                max_attempts: int=3
                ) -> None:
        """
        El constructor para la clase MongoLoggingDBHandler.

        Parameters:
            mongo_uri (str): Direccion URL para la conexion a MongoDB.
            db_name (str): Nombre de la base de datos en MongoDB.
            collection_logs (str): Nombre de la coleccion.
            buffer_size (int): Tamanio del buffer.
            waiting_time (float): Tiempo en segundos de espera entre intentos (default=1).
            max_attempts (int): Numero de veces que se intenta guardar un buffer de logs en caso de error (default=3).
        """
        super().__init__()
        self.client: MongoClient=MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self.db_name = self.client[db_name]
        self.collection_logs = self.db_name[collection_logs]
        self.buffer: List[Dict[str, Any]]=[]
        self.buffer_size = buffer_size
        self.waiting_time = waiting_time
        self.max_attempts = max_attempts

    def emit(self, record: logging.LogRecord) -> None:
        """
        Procesa cada registro de log y lo guarda en el buffer.

        Parameters:
            record (logging.LogRecord): Objeto LogRecord generado por logging.

        Raises:
            OperationFailure: Si falla la operacion con la base de datos.
            PyMongoError: Para cualquier otro error relacionado con PyMongo.
            Exception: En caso de cualquier otro error.
        """
        try:
            log_data = {
                "timestamp": datetime.strptime(record.asctime, '%Y-%m-%d %H:%M:%S'),
                "level": record.levelname,
                "module": record.module,
                "message": record.getMessage(),
                "function": record.funcName,
                "line": record.lineno
            }

            self.buffer.append(log_data)

            if len(self.buffer) >= self.buffer_size:
                self.flush()
        except (OperationFailure, PyMongoError) as e:
            print(f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')} - ERROR - {__name__} - No se pudo procesar el log: {e}")
        except Exception as e:
            print(f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')} - ERROR - {__name__} - Error inesperado al procesar el log: {e}")

    def flush(self) -> None:
        """
        Vacia el buffer e inserta estos logs en MongoDB.

        Raises:
            BulkWriteError: Clase de excepción para errores de escritura masiva..
            OperationFailure: Si falla la operacion con la base de datos.
            PyMongoError: Para cualquier otro error relacionado con PyMongo.
            Exception: En caso de cualquier otro error.
        """
        if not self.buffer:
            return

        for attempt in range(self.max_attempts + 1):
            try:
                self.collection_logs.insert_many(self.buffer, ordered=False)
                print(f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')} - INFO - {__name__} - Buffer de {len(self.buffer)} logs guardados correctamente.")
                self.buffer.clear()
                return
            except (BulkWriteError) as e:
                indices_fallidos = [error["index"] for error in e.details.get("writeErrors", [])]
                self.buffer = [self.buffer[i] for i in indices_fallidos]

                if not self.buffer:
                    return

                if attempt < self.max_attempts:
                    print(f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')} - WARNING - {__name__} - Reintentando guardar {len(self.buffer)} logs fallidos (Intento {attempt + 1}/{self.max_attempts}).")
                    time.sleep(self.waiting_time)
                else:
                    print(f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')} - ERROR - {__name__} - No se pudo guardar el buffer de logs.")
                    self.buffer.clear()
                    break

            except (OperationFailure, PyMongoError) as e:
                print(f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')} - ERROR - {__name__} - No se pudo guardar el buffer de logs: {e}")
                break
            except Exception as e:
                print(f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')} - ERROR - {__name__} - Error inesperado al procesar el log: {e}")
                break

    def close(self) -> None:
        """
        Vacia el buffer antes de cerrar el handler.
        """
        self.flush()
        self.client.close()
        super().close()
