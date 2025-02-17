from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import logging

class MetadataExtractor:
    """
    Clase para extraer metadatos de archivos
    """

    def __init__(self) -> None:
        pass

    def extractor_data_file(self, archivo: Path) -> Optional[Dict[str, Any]]:
        """
        Extrae los metadatos del archivo.
        
        Args:
            archivo (Path): Ruta del archivo del cual se extraeran los metadatos.
        
        Returns:
            Dict[str,Any]: Diccionario con los metadatos del archivo.
        
        Raises:
            Exception: Si ocurre un error al extraer los metadatos.
        """
        try:
            if not archivo.exists():
                logging.warning(f"El archivo '{archivo}' no existe.")
                return None
            
            if not archivo.is_file():
                logging.warning(f"'{archivo.name}' no es un archivo.")
                return None
            
            metadatos = {
                "nombre_archivo": archivo.name,
                "tamanio_archivo": archivo.stat().st_size,
                "fecha_creacion": datetime.fromtimestamp(archivo.stat().st_ctime).strftime("%Y-%m-%d %H:%M:%S"),
                "fecha_modificacion": datetime.fromtimestamp(archivo.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "tipo_archivo": archivo.suffix
            }
            logging.info(f"Metadatos extraidos de '{archivo.name}'")
            return metadatos
        
        except OSError as e:
            logging.error(f"Error al extraer metadatos del archivo {archivo}")
            return None
        except Exception as e:
            logging.exception(f"Error inesperado al extraer metadatos del archivo {archivo}")
            return None