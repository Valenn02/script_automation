import logging
import sys
from pymongo import MongoClient
from pymongo.errors import PyMongoError, OperationFailure, BulkWriteError
from datetime import datetime

class MongoLoggingDBHandler(logging.Handler):
    """
    Handler personalizado para enviar logs a MongoDB.

    Attributes:
        mongo_uri (str): Direccion URL para la conexion a MongoDB.
        db_name (str): Nombre de la base de datos en MongoDB.
        collection_name (str): Nombre de la coleccion.
        buffer (list): Lista para almacenar logs.
    """

    def __init__(self,
                 mongo_uri: str,
                 db_name: str,
                 collection_name: str,
                 buffer_size: int=50,
                ) -> None:
        """
        El constructor para la clase MongoLoggingDBHandler.

        Parameters:
            mongo_uri (str): Direccion URL para la conexion a MongoDB.
            db_name (str): Nombre de la base de datos en MongoDB.
            collection_name (str): Nombre de la coleccion.
        """
        super().__init__()
        self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.buffer = []
        self.buffer_size: int=buffer_size

    def emit(self, record: logging.LogRecord) -> None:
        """
        Procesa cada registro de log y lo guarda en el buffer.

        Parameters:
            record (logging.LogRecord): Objeto LogRecord generado por logging.
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

    def flush(self, max_attempts: int=3):
        """
        Vacia el buffer e inserta estos logs en MongoDB

        Parameters:
            max_attempts (int): Numero de veces que se intenta guardar un buffer de logs en caso de error.
        """
        if not self.buffer:
            return

        for attempt in range(max_attempts + 1):
            try:
                self.collection.insert_many(self.buffer, ordered=False)
                print(f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')} - INFO - {__name__} - Buffer de {len(self.buffer)} logs guardados correctamente.")
                self.buffer.clear()
                return
            except (BulkWriteError) as e:
                indices_fallidos = [error["index"] for error in e.details.get("writeErrors", [])]
                self.buffer = [self.buffer[i] for i in indices_fallidos]

                if not self.buffer:
                    return

                if attempt < max_attempts:
                    print(f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')} - WARNING - {__name__} - Reintentando guardar {len(self.buffer)} logs fallidos (Intento {attempt + 1}/{max_attempts}).")
                else:
                    print(f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')} - ERROR - {__name__} - No se pudo guardar el buffer de logs.")
                    self.buffer.clear()
                    break

            except (OperationFailure, PyMongoError) as e:
                print(f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')} - ERROR - {__name__} - No se pudo guardar el buffer de logs: {e}")
                break
            except Exception:
                print(f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')} - ERROR - {__name__} - Error inesperado al procesar el log: {e}")
                break

    def close(self):
        """
        Vacia el buffer antes de cerrar el hanldler.
        """
        self.flush()
        self.client.close()
        super().close()
