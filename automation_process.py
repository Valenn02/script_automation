from standard_response import StandardResponse
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
from mongo_db import MongoDBHandler
from decompress import TarDecompressor
from file_reader import FileProcessor
from xml_to_dict import XMLConverter
from json_matcher import JsonMatcher
from metadata_extractor import MetadataExtractor
from pymongo.errors import PyMongoError, ConnectionFailure, ServerSelectionTimeoutError, OperationFailure
import time
import logging
import sys
import json
import shutil
import os

load_dotenv()
BASE_DIR = Path.cwd()
DIR_COMPRIMIDOS = BASE_DIR / "COMPRIMIDOS"
DIR_DESCOMPRIMIDOS = BASE_DIR / "DESCOMPRIMIDOS"
DIR_COMPLEMENTOS = BASE_DIR / "COMPLEMENTOS"
#DIR_JSON = Path.cwd() / "JSON"
DB_NAME = os.getenv("DB_NAME", "BOA_VUELOS")
TAR_COLLECTION = "TAR_PROCESADOS"
JSON_COLLECTION = "JSON_PROCESADOS"
MONGO_URL = os.getenv("CONNECTION_URL_MONGO", "")
BUFFER_SIZE = int(os.getenv("BUFFER_SIZE_LOGS", 40))
WAITING_TIME = float(os.getenv("WAITING_TIME", 1))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", 3))

class AutomationProcess:
    """
    Clase para iniciar el proceso de automatizacion.

    Attributes:
        dir_comprimidos: Directorio de archivos comprimidos.
        dir_descomprimidos: Directorio de archivos descromprimidos.
        dir_complementos: Directorio de archivos complementos.
        mongo_uri: URI de conexion a MongoDB.
        db_name: Nombre de la base de datos.
        json_collection: str = Nombre de la coleccion para JSON procesados.
        tar_collection: str = Nombre de la coleccion para TAR procesados.
    """
    def __init__(self,
                dir_comprimidos: Path = DIR_COMPRIMIDOS,
                dir_descomprimidos: Path = DIR_DESCOMPRIMIDOS,
                dir_complementos: Path = DIR_COMPLEMENTOS,
                #dir_json: Path = DIR_JSON,
                mongo_uri: str = MONGO_URL,
                db_name: str = DB_NAME,
                json_collection: str = JSON_COLLECTION,
                tar_collection: str = TAR_COLLECTION
                ) -> None:
        """
        Constructor para la clase AutomationProcess

        Parameters:
            dir_comprimidos: Directorio de archivos comprimidos.
            dir_descomprimidos: Directorio de archivos descromprimidos.
            dir_complementos: Directorio de archivos complementos.
            mongo_uri: URI de conexion a MongoDB.
            db_name: Nombre de la base de datos.
            json_collection: str = Nombre de la coleccion para JSON procesados.
            tar_collection: str = Nombre de la coleccion para TAR procesados.
        """
        self.dir_comprimidos = dir_comprimidos
        self.dir_descomprimidos = dir_descomprimidos
        self.dir_complementos = dir_complementos
        #self.dir_json = dir_json
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.json_collection = json_collection
        self.tar_collection = tar_collection
        #self._mongo_client: Optional[MongoDBHandler] = None

    def ejecutar(self) -> Dict[str,Any]:
        """
        Ejecuta el proceso de automatizacion.

        Returns:
            Dict[str,Any]: Diccionario con los datos del proceso.
        """
        datos_proceso: Dict[str, Any] = {
            "num_tar": 0,
            "num_dict": 0,
        }

        # --- creacion de directorios ---
        self._create_directories()

        # --- validar directorio ---
        if not self._validate_directory(self.dir_comprimidos, "TAR"):
            shutil.rmtree(self.dir_descomprimidos)
            return datos_proceso

        # --- cargar archivos complementarios ---
        dict_codigos = self._load_plugins(self.dir_complementos)

        # --- conexion a MongoDB ---
        if not self._connect_to_mongodb():
            shutil.rmtree(self.dir_descomprimidos)
            return datos_proceso

        # --- descompresion de archivos ---
        if not self._unzip_files(self.dir_descomprimidos):
            resultado = self._mongo_client.disconnect()
            logging.info(resultado.message)
            return datos_proceso

        # --- procesamiento de archivos ---
        try:
            datos_proceso = self._process_uncompressed_files(self.dir_descomprimidos, dict_codigos, datos_proceso)
        except Exception as e:
            logging.error(f"Error inesperado durante el procesamiento: {e}")
            shutil.rmtree(self.dir_descomprimidos) # cuidado
        finally:
            if self._mongo_client:
                resultado = self._mongo_client.disconnect()
                logging.info(resultado.message)
                #logging.info("Conexion a MongoDB cerrada.")

        return datos_proceso

    def _connect_to_mongodb(self) -> bool:
        """Establece la conexion con MongoDB"""
        try:
            logging.info("Conectando a MongoDB.")
            self._mongo_client = MongoDBHandler(self.mongo_uri, self.db_name, self.tar_collection, self.json_collection)
            conexion = self._mongo_client.check_connect()
            logging.info(conexion.message)
            return True
        except Exception as e:
            #logging.error(f"Error al conectar con MongoDB: {e}")
            return False

    def _create_directories(self) -> None:
        """Crea los directorios necesarios si no existen."""
        try:
            self.dir_comprimidos.mkdir(parents=True, exist_ok=True)
            self.dir_complementos.mkdir(parents=True, exist_ok=True)
            self.dir_descomprimidos.mkdir(parents=True, exist_ok=True)
            logging.info("Directorios necesarios creados correctamente.")
        except OSError as e:
            logging.error(f"No se pudieron crear los directorios necesarios: {e}")
            sys.exit(1)

    def _validate_directory(self, directorio: Path, tipo_archivo: str) -> bool:
        """Valida si un directorio contiene archivos."""
        if not any(directorio.iterdir()):
            logging.warning(f"El directorio '{directorio.name}' no contiene archivos {tipo_archivo} para descomprimir.")
            return False
        return True

    def _load_plugins(self, directorio: Path) -> Dict[str,Any]:
        """Carga y combina los archivos complementarios"""
        if not self._validate_directory(directorio, "complementarios"):
            logging.warning(f"Se encontraron 0 codigos con sus descripciones.")
            return {}

        archivos_complementarios = list(directorio.iterdir())
        dict_codigos: Any = FileProcessor().merge_dictionaries(archivos_complementarios)
        logging.info(dict_codigos.message)
        logging.info(f"Se encontraron {len(dict_codigos.data)} codigos con sus descripciones.")
        return dict_codigos.data

    def _unzip_files(self, directorio: Path) -> bool:
        """Descomprime los archivos TAR.GZ"""
        try:
            descompresor = TarDecompressor(directorio)
            for archivo_tar in self.dir_comprimidos.glob("*.tar.gz"):
                resultado = self._mongo_client.check_processed_tar_file(archivo_tar.name)
                if resultado.success and resultado.data:
                    logging.info(f"{resultado.message} Ignorándolo.")
                    continue

                logging.info(f"Descomprimiendo '{archivo_tar.name}'.")
                resultado = descompresor.decompress_tar_gz(archivo_tar)
                logging.info(resultado.message)
            return True
        except Exception as e:
            logging.error(f"Error durante la descompresion: {e}")
            return False

    def _process_data_file(self, archivo_xml: Path, dict_codigos: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Procesa un archivo .DATA, lo convierte a diccinario y lo combina con los codigos - descripciones"""
        resultado: Any = self._xml_converter.transform_xml_to_dict(archivo_xml)
        if resultado.success:
            logging.info(resultado.message)
            try:
                str_contenido = json.loads(resultado.data)
                dict_combinado =  JsonMatcher().add_description_json(str_contenido, dict_codigos)
                metadata: Any = self._metadata_extractor.metadata_extractor(archivo_xml)
                if metadata.success:
                    metadata.data["contenido"] = dict_combinado
                return metadata.data
            except Exception as e:
                logging.error(f"Error al procesar el archivo '{archivo_xml.name}': {e}")
                return None
        return None

    def _process_manifest_file(self, archivo_xml: Path) -> Optional[str]:
        """Procesa un archivo .manifest y extrae su contenido."""
        resultado = self._xml_converter.format_manifest_file(archivo_xml)
        if resultado.success:
            return resultado.data
        return None

    def _process_uncompressed_files(self,  dir_descomprimidos: Path, dict_codigos: Dict[str, Any], datos_proceso: Dict[str, Any]) -> Dict[str, Any]:
        """Procesa los archivos XML descomprimidos, los convierte a diccionarios y los guarda en MongoDB."""
        for carpeta in dir_descomprimidos.iterdir():
            if not carpeta.is_dir():
                logging.info(f"'{carpeta.name}' no es una carpeta, ignorandolo.")
                continue

            datos_tar: Dict[str, Any] = {
                "nombre": carpeta.name,
                "fecha_procesado": datetime.now(),
                "manifest": None
            }
            documentos_procesados: List[Dict[str, Any]] = []

            logging.info(f"Procesando carpeta: '{carpeta.name}'")
            self._xml_converter = XMLConverter()
            self._metadata_extractor = MetadataExtractor()

            # para modificar la forma de clasificar archivos solo modificar 'clasificar_archivos_xml'
            lista_archivos: Any = self._xml_converter.build_list(carpeta)
            for archivo_xml in lista_archivos.data:
                stats = archivo_xml.stat()
                fecha_creacion = datetime.fromtimestamp(stats.st_ctime)
                fecha_modificacion = datetime.fromtimestamp(stats.st_mtime)
                fecha_acceso = datetime.fromtimestamp(stats.st_atime)
                logging.info(f"Procesando archivo '{archivo_xml.name}'")
                logging.info(f"Tamanio: {stats.st_size} bytes")
                logging.info(f"Fecha de creacion: {fecha_creacion}")
                logging.info(f"Fecha de ultima modificacion: {fecha_modificacion}")
                logging.info(f"Fecha de ultimo acceso: {fecha_acceso}")

                if archivo_xml.suffix.lower() == ".data":
                    dict_documento = self._process_data_file(archivo_xml, dict_codigos)
                    if dict_documento:
                        logging.info(f"Archivo '{archivo_xml.name}' procesado correctamente.")
                        documentos_procesados.append(dict_documento)

                elif archivo_xml.suffix.lower() == ".manifest":
                    contenido_manifest = self._process_manifest_file(archivo_xml)
                    if contenido_manifest:
                        datos_tar["manifest"] = contenido_manifest
                        logging.info(f"Archivo '{archivo_xml.name}' procesado correctamente.")

            if documentos_procesados:
                #ids_guardados: Any = self._mongo_client.save_documents(documentos_procesados)
                ids_guardados = self.save_documents_in_batch(documentos_procesados)
                datos_proceso["num_dict"] += len(ids_guardados)
                #logging.info(ids_guardados)
            resultado = self._mongo_client.save_processed_tar_file(datos_tar)
            logging.info(resultado.message)
            datos_proceso["num_tar"] += 1

        shutil.rmtree(self.dir_descomprimidos)
        return datos_proceso

    def save_documents_in_batch(self, documentos_procesados: List[Any]) -> List[Any]:
        """Guarda los documentos por lotes."""
        inserted_ids = []
        global_errors = []

        for i in range(0, len(documentos_procesados), BUFFER_SIZE):
            batch = documentos_procesados[i:i + BUFFER_SIZE]
            lote_num = i // BUFFER_SIZE + 1

            invalid_documents = [doc for doc in batch if not isinstance(doc, dict)]
            valid_batch = [doc for doc in batch if isinstance(doc, dict)]

            if invalid_documents:
                logging.warning(f"Lote {lote_num}: Se ingnoraron {len(invalid_documents)} elementos no validos.")

            if not valid_batch:
                logging.warning(f"Lote {lote_num}: No quedan documentos validos para insertar.")
                continue

            last_error = None
            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    result: Any = self._mongo_client.save_documents(valid_batch)
                    inserted_ids.extend(result.data)
                    logging.info(f"Lote {lote_num}: {len(result.data)} documentos guardados correctamente (Intento {attempt}/{MAX_ATTEMPTS}).")
                    last_error = None
                    break
                except (OperationFailure, PyMongoError) as e:
                    last_error = e
                    if attempt < MAX_ATTEMPTS:
                        logging.warning(f"Lote {lote_num}: Error en intento {attempt}/{MAX_ATTEMPTS}. Reintentando en {WAITING_TIME}s ... Error: {e}")
                        time.sleep(WAITING_TIME)
                    else:
                        logging.error(f"Lote {lote_num}: Fallo despues de {MAX_ATTEMPTS} intentos. Error: {e}")

            if last_error:
                global_errors.append(f"Lote {lote_num}: {last_error}")

        if global_errors:
            error_msg = f"Errores en {len(global_errors)} lotes: {', '.join(global_errors)}"
            logging.error(f"{error_msg}. Documentos insertados con exito: {len(inserted_ids)}")

        logging.info(f"Todos los documentos validos insertados con exito. Total: {len(inserted_ids)}")
        return inserted_ids
