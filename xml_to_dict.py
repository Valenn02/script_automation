import xmltodict
import json
import logging
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any

#DIRECTORIO_JSON = Path.cwd() / "JSON"

class XMLConverter:
    """
    Clase para convertir archivos XML a Diccionarios (y formatear archivos manifest).
    """
    
    #def __init__(self, directorio_json: Path = DIRECTORIO_JSON) -> None:
    #    self.directorio_json = directorio_json
    #    self.directorio_json.mkdir(parents=True, exist_ok=True)
    def __init__(self) -> None:
        """
        Inicializa el convertidor XML.
        """
        pass

    def _agregar_salto_de_linea(self, cadena: str) -> str:
        """
        Agrega un salto de linea a un cadena, o lo elimina si ya existe.
        
        Args:
            cadena (str): Cadena de texto.
            
        Returns:
            str: Si la cadena modificada.
        """
        return cadena + "\n" if not cadena.endswith("\n") else cadena.replace("\n", "")

    def formatear_archivo_manifest(self, archivo: Path) -> Optional[str]:
        """
        Formatea el texto de un archivo .manifest.
        
        Args:
            archivo (Path): Archivo Manifest.
        
        Returns:
            str: Contenido del archivo.
        
        Raises:
            FileNotFoundError: Si el archivo no existe.
            Exception: Si ocurre un error al transformar un archivo.
        """
        try:
            if not archivo.suffix.lower() == ".manifest":
                logging.warning(f"El archivo '{archivo.name}' no tiene formato .manifest")
                return None
            
            logging.info(f"Procesando archivo '{archivo.name}'")
            with open(archivo, "r", encoding="utf-8") as archivo_manifest:
                contenido_manifest = archivo_manifest.read()
            
            #with open((self.directorio_json / archivo.name), "w", encoding="utf-8") as manifest_final:
            #    manifest_final.write(self._formatear_archivo_manifest(contenido_manifest))
                
            manifest_data = "".join(list(map(self._agregar_salto_de_linea, contenido_manifest.split(" "))))
            return manifest_data
        
        except FileNotFoundError:
            logging.error(f"Archivo no encontrado: '{archivo}'")
            return None
        except Exception as e:
            logging.exception(f"Error al procesar el archivo '{archivo.name}': {e}")
            return None

    def construir_lista(self, carpeta_path: Path) -> List[Path]:
        """
        Construye la lista de archivos .DATA a procesar.
        
        Args:
            directorio (Path): Directorio con los archivos a clasificar.
        Returns:
            List[Path]: Lista con los archivos clasificados.
        """
    
        resultados: List[Path] = []
        parejas_dict: Dict[str, Any] = self._emparejar_archivos_data(carpeta_path)
        
        # implementar logica de combinacion de archivos (opcional)
        # --------------------------------------------------------
        for par in parejas_dict.values():
            if par["complemento"]:
                resultados.append(par["complemento"])
            elif par["original"]:
                resultados.append(par["original"])
        # --------------------------------------------------------
        
        manifest = list(carpeta_path.glob("*.manifest"))
        return resultados + manifest
    
    def _emparejar_archivos_data(self, carpeta_path: Path) -> Dict[str, Dict[str, Optional[Path]]]:
        """
        Empareja los archivos .DATA (originales y complementos).
        
        Args:
            directorio (Path): Directorio con los archivos a clasificar.
        
        Returns:
            Dict[str,Any]: Diccionario con los archivos originales y complementos emparejados.
        """
        parejas: Dict[str, Dict[str, Optional[Path]]] = {}
        
        for archivo_path in carpeta_path.glob("*.DATA"):
            nombre_archivo = archivo_path.name
            
            if ".1." in nombre_archivo:
                base_nombre = nombre_archivo.replace(".1.", ".X.")
                if base_nombre not in parejas:
                    parejas[base_nombre] = {"original": archivo_path, "complemento": None}
                else:
                    parejas[base_nombre]["original"] = archivo_path
            elif ".P." in nombre_archivo:
                base_nombre = nombre_archivo.replace(".P.", ".X.")
                if base_nombre not in parejas:
                    parejas[base_nombre] = {"original": None, "complemento": archivo_path}
                else:
                    parejas[base_nombre]["complemento"] = archivo_path
            else:
                logging.warning(f"El archivo '{archivo_path.name}' no tiene una correcta nomenclatura en el nombre, ignorandolo ...")
        return parejas
    
    def transformar_xml_a_dict(self, archivo: Path) -> Optional[str]:
        """
        Transforma el contenido de un archivo XML a Diccionario.
        
        Args:
            archivo (Path): Archivo XML a transformar.
        
        Returns:
            str: Contenido del archivo XML trasnformado.
        
        Raises:
            FileNotFoundError: Si el archivo no existe.
            xmltodict.ParsingInterrupted: Si el arhcivo XML no tiene un formato valido.
            Exception: Si ocurre un error al transformar un archivo.
        """

        try:
            if not archivo.suffix.lower() == ".data":
                logging.warning(f"El archivo '{archivo.name}' no tiene la extension .DATA")
                return None
                
            logging.info(f"Procesando archivo '{archivo.name}'")
            with open(archivo, 'r', encoding="utf-8") as archivo_xml:
                xml_contenido = archivo_xml.read()

            xml_dict = xmltodict.parse(xml_contenido)
            #json_data = json.dumps(xml_dict, indent=4)
            dict_data = json.dumps(xml_dict)
            #archivo_json_name = archivo.with_suffix(".json").name

            #with open((self.directorio_json / archivo_json_name), 'w', encoding="utf-8") as archivo_json:
            #    archivo_json.write(json_data)
                
            return dict_data

        except FileNotFoundError:
            logging.error(f"Archivo no encontrado: '{archivo}'.")
            return None
        except xmltodict.ParsingInterrupted as e:
            logging.error(f"Error al parsear XML en '{archivo.name}': {e}.")
            return None
        except Exception as e:
            logging.exception(f"Error al procesar el archivo '{archivo.name}': {e}")
            return None