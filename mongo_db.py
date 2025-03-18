from standard_response import StandardResponse
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ConnectionFailure, ServerSelectionTimeoutError, OperationFailure
from typing import Dict, List, Any

class MongoDBHandler:
    """
    Clase handler para interactuar y operar con la base de datos MongoDB.

    Attributes:
        client (MongoClient): Cliente de conexion a MongoDB.
        db_name (str): Nombre de la base de datos en MongoDB.
        collection_tar (str): Nombre de la coleccion para guardar metadatos de archivos TAR.
        collection_json (str): Nombre de la coleccion para guardar metadatos de los archivos JSON.
    """

    def __init__(self,
                mongo_uri: str,
                db_name: str,
                collection_tar: str,
                collection_json: str,
                ) -> None:
        """
        Constructor para la clase MongoDBHandler.

        Parameters:
            mongo_uri (str): Direccion URL para la conexion a MongoDB.
            db_name (str): Nombre de la base de datos en MongoDB.
            collection_tar (str): Nombre de la coleccion para guardar metadatos de archivos TAR.
            collection_json (str): Nombre de la coleccion para guardar metadatos de los archivos JSON.
        """
        self.client: MongoClient=MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self.db_name = self.client[db_name]
        self.collection_tar = self.db_name[collection_tar]
        self.collection_json = self.db_name[collection_json]

    def check_connect(self) -> StandardResponse:
        """
        Comprueba la conexion con la base de datos de MongoDB.

        Returns:
            StandardResponse: Clase estandar para encapsular respuestas de funciones.

        Raises:
            ServerSelectionTimeoutError: Si el servidor no responde en el tiempo esperado.
            ConnectionFailure: Si no se puede establecer una conexion con el servidor.
            PyMongoError: Para cualquier otro error relacionado con PyMongo.
        """
        try:
            self.client.admin.command("ping")
            return StandardResponse(
                success=True,
                message="Conexion exitosa con MongoDB.",
                )
        except (ServerSelectionTimeoutError, ConnectionFailure, PyMongoError) as e:
            return StandardResponse(
                success=True,
                message="No se pudo conectar a MongoDB.",
                error_details=str(e)
                )

    def save_documents(self, documents: List[Dict[str, Any]]) -> StandardResponse:
        """
        Guarda los documentos en una coleccion de MongoDB.

        Parameters:
            documents (List[Dict[str, Any]]): Lista de diccionarios a guardar.

        Returns:
            StandardResponse: Clase estandar para encapsular respuestas de funciones.

        Raises:
            OperationFailure: Si falla la operacion con la base de datos.
            PyMongoError: Para cualquier otro error relacionado con PyMongo.
        """
        if not self.client:
            return StandardResponse(
                success=False,
                message="No hay una conexion establecida con MongoDB.",
            )

        if not documents:
            return StandardResponse(
                success=True,
                message="Buffer de documentos vacio. No se realizaron inserciones."
            )

        try:
            resultado = self.collection_json.insert_many(documents, ordered=False)
            return StandardResponse(
                success=True,
                data=resultado.inserted_ids,
                message=f"Se insertaron {len(resultado.inserted_ids)} documentos en '{self.collection_json.name}'"
            )
        except (OperationFailure, PyMongoError) as e:
            return StandardResponse(
                success=False,
                message="Error al guardar los documentos en MongoDB.",
                error_details=str(e)
            )

    def check_processed_tar_file(self, file_name: str) -> StandardResponse:
        """
        Verifica en MongoDB si un archivo TAR ya fue procesado.

        Parameters:
            file_name (str): Nombre del archivo TAR a verificar.

        Returns:
            StandardResponse: Clase estandar para encapsular respuestas de funciones.

        Exceptions:
            OperationFailure: Si falla la operacion con la base de datos.
            PyMongoError: Para cualquier otro error relacionado con PyMongo.
        """
        if not self.client:
            return StandardResponse(
                success=False,
                message="No hay una conexion establecida con MongoDB.",
            )

        try:
            resultado = self.collection_tar.find_one({"nombre": file_name})
            mensaje1 = f"El archivo '{file_name}' SI fue procesado previamente."
            mensaje2 = f"El archivo '{file_name}' NO fue procesado previamente."
            return StandardResponse(
                success=True,
                data=resultado,
                message=mensaje1 if resultado else mensaje2
            )
        except (OperationFailure, PyMongoError) as e:
            return StandardResponse(
                success=False,
                message=f"Error al verificar el archivo '{file_name}' en MongoDB.",
                error_details=str(e)
            )

    def save_processed_tar_file(self, diccionario_data: Dict[str, Any]) -> StandardResponse:
        """
        Guarda metadatos del archivo TAR procesado en MongoDB.

        Parameters:
            diccionario_data (Dict[str, Any]): Diccionario con metadatos del archivo TAR.

        Returns:
            StandardResponse: Clase estandar para encapsular respuestas de funciones.

        Exceptions:
            OperationFailure: Si falla la operacion con la base de datos.
            PyMongoError: Para cualquier otro error relacionado con PyMongo.
        """
        if not self.client:
            return StandardResponse(
                success=False,
                message="No hay una conexion establecida con MongoDB.",
            )

        try:
            resultado = self.collection_tar.insert_one(diccionario_data)
            return StandardResponse(
                success=True,
                data=resultado,
                message=f"Metadatos del archivo TAR '{diccionario_data['nombre']}' guardados en '{self.collection_tar.name}'."
            )
        except (OperationFailure, PyMongoError) as e:
            return StandardResponse(
                success=False,
                message="Error al guardar metadatos del archivo TAR en MongoDB.",
                error_details=str(e)
            )

    def disconnect(self) -> StandardResponse:
        """
        Cierra la conexion con MongoDB.

        Returns:
            StandardResponse: Clase estandar para encapsular respuestas de funciones.
        """
        try:
            if self.client:
                self.client.close()
            return StandardResponse(
                success=True,
                message="Conexion cerrada a MongoDB."
            )
        except (PyMongoError) as e:
            return StandardResponse(
                success=False,
                message="No se pudo cerrar la conexion a MongoDB.",
                error_details=str(e)
            )
