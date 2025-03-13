from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
from mongo_db import MongoDBHandler, MongoDBConnectionError
from decompress import TarDecompressor
from file_reader import FileProcessor
from xml_to_dict import XMLConverter
from json_matcher import JsonMatcher
from metadata_extractor import MetadataExtractor
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

class AutomationProcess:
    
    def __init__(self,
                dir_comprimidos: Path = DIR_COMPRIMIDOS,
                dir_descomprimidos: Path = DIR_DESCOMPRIMIDOS,
                dir_complementos: Path = DIR_COMPLEMENTOS,
                #dir_json: Path = DIR_JSON,
                mongo_uri: str = MONGO_URL,
                db_name: str = DB_NAME,
                json_collection: str = JSON_COLLECTION,
                tar_collection: str = TAR_COLLECTION) -> None:
        """
        Inicializa la clase AutomationProcess
        
        Args:
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
        self._crear_directorios()
        
        # --- validar directorio ---
        if not self._validar_directorio(self.dir_comprimidos, "TAR"):
            shutil.rmtree(self.dir_descomprimidos)
            return datos_proceso
        
        # --- cargar archivos complementarios ---
        dict_codigos = self._cargar_complementos(self.dir_complementos)
        
        # --- conexion a MongoDB ---
        if not self._conectar_a_mongodb():
            shutil.rmtree(self.dir_descomprimidos)
            return datos_proceso
        
        # --- descompresion de archivos ---
        if not self._desomprimir_archivo(self.dir_descomprimidos):
            self._mongo_client.cerrar_conexion()
            return datos_proceso
        
        # --- procesamiento de archivos ---
        try:
            datos_proceso = self._procesar_archivos_descomprimidos(self.dir_descomprimidos, dict_codigos, datos_proceso)
        except Exception as e:
            logging.exception(f"Error inesperado durante el procesamiento: {e}")
            shutil.rmtree(self.dir_descomprimidos) # cuidado
        finally:
            if self._mongo_client:
                self._mongo_client.cerrar_conexion()
                #logging.info("Conexion a MongoDB cerrada.")
        
        return datos_proceso

    def _conectar_a_mongodb(self) -> bool:
        """Establece la conexion con MongoDB"""
        try:
            logging.info("Conectando a MongoDB.")
            self._mongo_client = MongoDBHandler(self.mongo_uri, self.db_name, self.tar_collection, self.json_collection)
            self._mongo_client.conectar()
            #logging.info("Conexion exitosa a MongoDB.")
            return True
        except MongoDBConnectionError as e:
            #logging.error(f"Error al conectar con MongoDB: {e}")
            return False
        
    def _crear_directorios(self) -> None:
        """Crea los directorios necesarios si no existen."""
        try:
            self.dir_comprimidos.mkdir(parents=True, exist_ok=True)
            self.dir_complementos.mkdir(parents=True, exist_ok=True)
            self.dir_descomprimidos.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logging.error(f"No se pudieron crear los directorios necesarios: {e}")
            sys.exit(1)

    def _validar_directorio(self, directorio: Path, tipo_archivo: str) -> bool:
        """Valida si un directorio contiene archivos."""
        if not any(directorio.iterdir()):
            logging.warning(f"El directorio '{directorio.name}' no contiene archivos {tipo_archivo} para descomprimir.")
            return False
        return True
    
    def _cargar_complementos(self, directorio: Path) -> Dict[str,Any]:
        """Carga y combina los archivos complementarios"""
        if not self._validar_directorio(directorio, "complementarios"):
            logging.warning(f"Se encontraron 0 codigos con sus descripciones.")
            return {}
        
        archivos_complementarios = list(self.dir_complementos.iterdir())
        dict_codigos = FileProcessor().combinar_diccionarios(archivos_complementarios)
        logging.info(f"Se encontraron {len(dict_codigos)} codigos con sus descripciones.")
        return dict_codigos
    
    def _desomprimir_archivo(self, directorio: Path) -> bool:
        """Descomprime los archivos TAR.GZ"""
        try:
            descompresor = TarDecompressor(directorio)
            for archivo_tar in self.dir_comprimidos.glob("*.tar.gz"):
                if self._mongo_client.verificar_archivo_procesado(archivo_tar.name):
                    logging.info(f"El archivo '{archivo_tar.name}' ya fue procesado (encontrado en la base de datos), ignorándolo.")
                    continue
                
                logging.info(f"Descomprimiendo '{archivo_tar.name}'.")
                descompresor.descomprimir_tar_gz(archivo_tar)
            return True
        except Exception as e:
            logging.error(f"Error durante la descompresion: {e}")
            return False
    
    def _procesar_archivo_data(self, archivo_xml: Path, dict_codigos: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Procesa un archivo .DATA, lo convierte a diccinario y lo combina con los codigos - descripciones"""
        resultado = self._xml_converter.transformar_xml_a_dict(archivo_xml)
        if resultado:
            try:
                str_contenido = json.loads(resultado)
                dict_combinado =  JsonMatcher().agregar_descripcion_json(str_contenido, dict_codigos)
                metadata = self._metadata_extractor.extractor_data_file(archivo_xml)
                if metadata:
                    metadata["contenido"] = dict_combinado
                return metadata
            except Exception as e:
                logging.error(f"Error al procesar el archivo '{archivo_xml.name}': {e}")
                return None
        return None
    
    def _procesar_archivo_manifest(self, archivo_xml: Path) -> Optional[str]:
        """Procesa un archivo .manifest y extrae su contenido."""
        resultado = self._xml_converter.formatear_archivo_manifest(archivo_xml)
        if resultado:
            contenido_archivo = resultado
            return contenido_archivo
        return None
    
    def _procesar_archivos_descomprimidos(self,  dir_descomprimidos: Path, dict_codigos: Dict[str, Any], datos_proceso: Dict[str, Any]) -> Dict[str, Any]:
        """Procesa los archivos XML descomprimidos, los convierte a diccionarios y los guarda en MongoDB."""
        for carpeta in dir_descomprimidos.iterdir():
            if not carpeta.is_dir():
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
            for archivo_xml in self._xml_converter.construir_lista(carpeta):
                #logging.info(f"Procesando archivo '{archivo_xml.name}'")
                
                if archivo_xml.suffix.lower() == ".data":
                    dict_documento = self._procesar_archivo_data(archivo_xml, dict_codigos)
                    if dict_documento:
                        #logging.info(f"- Archivo {tipo_archivo} procesado.")
                        documentos_procesados.append(dict_documento)
                
                elif archivo_xml.suffix.lower() == ".manifest":
                    contenido_manifest = self._procesar_archivo_manifest(archivo_xml)
                    if contenido_manifest:
                        datos_tar["manifest"] = contenido_manifest
                        #logging.info(f"- Archivo {tipo_archivo} procesado.")
            
            if documentos_procesados:
                ids_guardados = self._mongo_client.guardar_diccionarios(documentos_procesados)
                datos_proceso["num_dict"] += len(ids_guardados)
                #logging.info(f"Se guardaron {len(ids_guardados)} documentos en MongoDB.")
            self._mongo_client.guardar_tar_procesado(datos_tar)
            datos_proceso["num_tar"] += 1
            
        shutil.rmtree(self.dir_descomprimidos)
        return datos_proceso