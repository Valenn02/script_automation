from pathlib import Path
from typing import List
from standard_response import StandardResponse
from typing import Any
import csv
import json

FORMATOS_SOPORTADOS = [".json", ".txt", ".csv"]

class FileProcessor:
    """
    Clase para procesar archivos en un directorio y extraer los codigos con sus descripciones.
    """

    def __init__(self):
        pass

    def _load_file_content(self, file: Path) -> StandardResponse:
        """
        Carga el contenido de un archivo en un diccionario.

        Parameters:
            file (Path): Archivo con datos de codigos y descripciones.

        Returns:
            StandardResponse: Clase estandar para encapsular respuestas de funciones.

        Raises:
            json.JSONDecodeError: Si el arhcivo JSON no tiene un formato valido.
            csv.Error: Si hay un error al leer el archivo CSV.
            Exception: Si ocurre algun error inesperado.
        """
        try:
            if not file.exists():
                return StandardResponse(
                    success=False,
                    data={},
                    message=f"El archivo '{file.name}' no existe."
                )

            if not file.is_file():
                return StandardResponse(
                    success=False,
                    data= {},
                    message=f"'{file.name}' no es un archivo."
                )

            if file.match("*.txt"):
                diccionario_txt = {}
                with open(file, "r", encoding="utf-8") as archivo_txt:
                    for linea in archivo_txt:
                        if "=" not in linea:
                            continue
                        codigo, descripcion = linea.strip().split('=', 1)
                        diccionario_txt[codigo.strip()] = descripcion.strip()
                return StandardResponse(
                    success=True,
                    data=diccionario_txt,
                    message=f"Se cargaron los codigos y descripciones del archivo '{file.name}'."
                )

            elif file.match("*.json"):
                with open(file, "r", encoding="utf-8") as archivo_json:
                    json_content = json.load(archivo_json)
                return StandardResponse(
                    success=True,
                    data=json_content,
                    message=f"Se cargaron los codigos y descripciones del archivo '{file.name}'."
                )

            elif file.match("*.csv"):
                diccionario_csv = {}
                with open(file, "r", encoding="utf-8") as archivo_csv:
                    contenido_csv = csv.reader(archivo_csv, delimiter=",")
                    next(contenido_csv, None)
                    for fila in contenido_csv:
                        if len(fila) < 2:
                            continue
                        diccionario_csv[fila[0].strip()] = fila[1].strip()
                return StandardResponse(
                    success=True,
                    data=diccionario_csv,
                    message=f"Se cargaron los codigos y descripciones del archivo '{file.name}'."
                )
            else:
                return StandardResponse(
                    success=False,
                    data={},
                    message=f"Formato de archivo no soportado: '{file.name}'."
                )

        except (json.JSONDecodeError, csv.Error, Exception) as e:
            return StandardResponse(
                success=False,
                data={},
                message=f"Error al procesar el archivo '{file.name}'.",
                error_details=str(e)
            )

    def merge_dictionaries(self, file_list: List[Path]) -> StandardResponse:
        """
        Combina el contenido de todos los archivos soportados en un solo diccionario.

        Parameters:
            file_list (Path): Lista de archivos a combinar.

        Returns:
            StandardResponse: Clase estandar para encapsular respuestas de funciones.
        """

        diccionarios_combinados = {}

        for archivo in file_list:
            if not isinstance(archivo, Path):
                continue

            if archivo.suffix.lower() not in FORMATOS_SOPORTADOS:
                continue

            diccionario_archivo: Any = self._load_file_content(archivo).data
            diccionarios_combinados.update(diccionario_archivo)

        return StandardResponse(
            success=True,
            data=diccionarios_combinados,
            message="Codigos de archivos combinados correctamente."
        )
