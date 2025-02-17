# module: file_reader
from pathlib import Path
from typing import Dict, List
import csv
import json
import logging

FORMATOS_SOPORTADOS = [".json", ".txt", ".csv"]

class FileProcessor:
    """
    Clase para procesar archivos en un directorio y extraer los codigos con sus descripciones.
    """

    def __init__(self):
        pass

    def _cargar_contenido_archivo(self, archivo: Path) -> Dict[str, str]:
        """
        Carga el contenido de un archivo en un diccionario.
        
        Args:
            archivo (Path): Archivo con datos de codigos y descripciones.
        
        Returns:
            Dict[str,Any]: Diccionario con el contenido del archivo.
        
        Raises:
            json.JSONDecodeError: Si el arhcivo JSON no tiene un formato valido.
            csv.Error: Si hay un error al leer el archivo CSV.
            Exception: Si ocurre algun error inesperado.
        """
        try:
            if not archivo.exists():
                logging.warning(f"El archivo '{archivo.name}' no existe.")
                return {}
            
            if not archivo.is_file():
                logging.warning(f"'{archivo.name}' no es un archivo.")
                return {}
            
            if archivo.match("*.txt"):
                diccionario_txt = {}
                with open(archivo, "r", encoding="utf-8") as archivo_txt:
                    for linea in archivo_txt:
                        if "=" not in linea:
                            logging.warning(f"Formato de linea invalido en '{archivo.name}': {linea.strip()}")
                            continue
                        codigo, descripcion = linea.strip().split('=', 1)
                        diccionario_txt[codigo.strip()] = descripcion.strip()
                return diccionario_txt
            
            elif archivo.match("*.json"):
                with open(archivo, "r", encoding="utf-8") as archivo_json:
                    return json.load(archivo_json)

            elif archivo.match("*.csv"):
                diccionario_csv = {}
                with open(archivo, "r", encoding="utf-8") as archivo_csv:
                    contenido_csv = csv.reader(archivo_csv, delimiter=",")
                    next(contenido_csv, None)
                    for fila in contenido_csv:
                        if len(fila) < 2:
                            logging.warning(f"Fila CSV invalida en '{archivo.name}': {fila}")
                            continue
                        diccionario_csv[fila[0].strip()] = fila[1].strip()
                return diccionario_csv
            else:
                logging.warning(f"Formato de archivo no soportado: '{archivo.name}'.")
                return {}

        except (json.JSONDecodeError, csv.Error, Exception) as e:
            logging.exception(f"Error al procesar el archivo '{archivo.name}': {e}")
            return {}
    
    def combinar_diccionarios(self, lista_archivos: List[Path]) -> Dict[str, str]:
        """
        Combina el contenido de todos los archivos soportados en un solo diccionario.
        
        Args:
            List (Path): Lista de archivos a combinar.
        
        Returns:
            Dict[str,Any]: Diccionario combinado con el contenido de todos los archivos.
        """

        diccionarios_combiandos = {}

        for archivo in lista_archivos:
            
                if not isinstance(archivo, Path):
                    logging.error(f"Se esperaba un objeto Path, se obtuvo: {type(archivo)}. Ignorando este elemento.")
                    continue

                if archivo.suffix.lower() not in FORMATOS_SOPORTADOS:
                    logging.warning(f"Archivo con formato no soportado: '{archivo.name}'. Ignorandolo ...")
                    continue

                logging.info(f"Procesando archivo: '{archivo.name}' ...")
                diccionario_archivo = self._cargar_contenido_archivo(archivo)
                diccionarios_combiandos.update(diccionario_archivo)

        return diccionarios_combiandos