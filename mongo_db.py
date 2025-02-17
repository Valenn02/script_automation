from pymongo import MongoClient
from pymongo.errors import PyMongoError, ConnectionFailure, ServerSelectionTimeoutError, OperationFailure
from typing import Dict, List, Any, Optional
import logging

class MongoDBHandler:
    """
    Clase para interactuar y operar con la base de datos MongoDB.
    
    Attributes:
        uri: Direccion URI de conexion.
        db_name: Nombre de la base de datos.
        collection_tar: Nombre de la coleccion para guardar metadatos de archivos TAR.
        collection_json: Nombre de la coleccion para guardar metadatos de los archivos JSON.
        client: Cliente de MongoDB (inicializado en 'conectar').
    """
    
    def __init__(self,
                uri: str,
                db_name: str,
                collection_tar: str,
                collection_json: str,) -> None:
        """
        Inicializa el manejador de MongoDB.
        
        Args:
            uri (str): URI de conexion a MongoDB.
            db_name (str): Nombre de la base de datos.
            collection_tar (str): Nombre de la coleccion para TAR procesados.
            collection_json (str): Nombre de la coleccion para JSON procesados.
            client (MongoClient): Cliente de MongoDB.
        """
        self.uri = uri
        self.db_name = db_name
        self.collection_tar = collection_tar
        self.collection_json = collection_json
        self.client: Optional[MongoClient] = None

    def conectar(self) -> None:
        """
        Conecta a MongoDB.
        
        Raises:
            MongoDBConnectionError: Error personalizado para problemas de conexion.
        """
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=3000)
            self.client.admin.command("ping")
            logging.info("Conexion a MongoDB exitosa.")
        except (ServerSelectionTimeoutError, ConnectionFailure, PyMongoError) as e:
            logging.error(f"No se pudo conectar a MongoDB: {e}")
            raise MongoDBConnectionError(f"No se pudo conectar a MongoDB: {e}") from e

    def guardar_diccionarios(self, documentos: List[Dict[str, Any]]) -> List[Any]:
        """
        Guarda los diccionarios dentro de una lista en una coleccion de MongoDB.
        
        Args:
            documentos (List[Dict[str,Any]]): Lista de diccionarios a guardar.
        
        Returns:
            List[Any]: Lista de los IDs de los diccionarios guardados.
        
        Raises:
            MongoDBOperationError: Error personalizado para problemas de guardado de datos.
        """
        if not self.client:
            raise MongoDBConnectionError("No hay una conexion activa a MongoDB.")
        
        try:
            db = self.client[self.db_name]
            coleccion = db[self.collection_json]
            resultado = coleccion.insert_many(documentos)
            logging.info(f"Se insertaron {len(resultado.inserted_ids)} documentos en '{self.collection_json}'")
            return resultado.inserted_ids
        except (OperationFailure, PyMongoError) as e:
            logging.error(f"No se pudo realizar la operacion: {e}") 
            raise MongoDBOperationError(f"Error al guardar documentos: {e}") from e

    def verificar_archivo_procesado(self, nombre_archivo: str) -> bool:
        """
        Verifica en MongoDB si una archivo TAR fue procesado.
        
        Args:
            nombre_archivo (str): Nombre del archivo a verificar.
        
        Returns:
            bool: True si el archivo TAR ya fue procesado, False en caso contrario.
        
        Raises:
            MongoDBOperationError: Error personalizado para problemas de consulta.
        """
        if not self.client:
            raise MongoDBConnectionError("No hay una conexion activa a MongoDB.")
        
        try:
            db = self.client[self.db_name]
            coleccion = db[self.collection_tar]
            resultado = coleccion.find_one({"nombre": nombre_archivo})
            logging.info(f"Verificando si '{nombre_archivo}' ya fue procesado.")
            return resultado is not None
        except (OperationFailure, PyMongoError) as e:
            logging.error(f"Error al verificar el archivo '{nombre_archivo}' en MongoDB: {e}")
            raise MongoDBOperationError(f"Error al verificar archivo procesado: {e}") from e

    def guardar_tar_procesado(self, diccionario_data: Dict[str, Any]) -> int:
        """
        Guarda metadatos del archivo TAR procesado.
        
        Args:
            nombre_carpeta (str): Nombre del archivo TAR procesado.
        
        Returns:
            int: ID del diccionario guardado.
        
        Raises:
            MongoDBOperationError: Error personalizado para problemas de consulta.
        """
        if not self.client:
            raise MongoDBConnectionError("No hay una conexion activa a MongoDB.")
        
        try:
            db = self.client[self.db_name]
            coleccion = db[self.collection_tar]
            resultado = coleccion.insert_one(diccionario_data)
            logging.info(f"Metadatos del archivo TAR '{diccionario_data['nombre']}' guardados en '{self.collection_tar}'.")
            return resultado.inserted_id
        except (OperationFailure, PyMongoError) as e:
            logging.error("Error al guardar metadatos del TAR en MongoDB: {e}")
            raise MongoDBOperationError(f"Error al guardar metadatos de TAR: {e}") from e

    def cerrar_conexion(self) -> None:
        """
        Cierra la conexion con MongoDB.
        """
        if self.client:
            self.client.close()
            logging.info("Conexion a MongoDB cerrada.")

# --- excepciones personalizadas ---
class MongoDBError(Exception):
    """Excepcion base para errores relacionados con MongoDB."""
    pass

class MongoDBConnectionError(MongoDBError):
    """Excepcion para errores de conexion a MongoDB."""
    pass

class MongoDBOperationError(MongoDBError):
    """Excepcion para errores de conexion a MongoDB."""
    pass