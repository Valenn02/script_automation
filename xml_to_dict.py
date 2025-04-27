import xmltodict
import json
from pathlib import Path
from typing import Optional, Dict
from standard_response import StandardResponse
from collections.abc import Mapping

#DIRECTORIO_JSON = Path.cwd() / "JSON"

class XMLConverter:
    """
    Clase para convertir archivos XML a Diccionarios.
    """

    #def __init__(self, directorio_json: Path = DIRECTORIO_JSON) -> None:
    #    self.directorio_json = directorio_json
    #    self.directorio_json.mkdir(parents=True, exist_ok=True)
    def __init__(self) -> None:
        pass

    def _add_line_break(self, text: str) -> StandardResponse:
        """
        Agrega un salto de linea a un cadena, o lo elimina si ya existe.

        Parameters:
            text (str): Cadena de texto.

        Returns:
            StandardResponse: Clase estandar para encapsular respuestas de funciones.
        """
        return StandardResponse(
            success=True,
            data=text + "\n" if not text.endswith("\n") else text.replace("\n", ""),
            message="Saltos de linea agregados correctamente."
        )

    def format_manifest_file(self, manifest_file: Path) -> StandardResponse:
        """
        Formatea el texto de un archivo .manifest.

        Parameters:
            manifest_file (Path): Archivo Manifest.

        Returns:
            StandardResponse: Clase estandar para encapsular respuestas de funciones.

        Raises:
            FileNotFoundError: Si el archivo no existe.
            Exception: Si ocurre un error al transformar un archivo.
        """
        try:
            if not manifest_file.suffix.lower() == ".manifest":
                return StandardResponse(
                    success=False,
                    message=f"El archivo '{manifest_file.name}' no tiene formato correcto."
                )

            with open(manifest_file, "r", encoding="utf-8") as archivo_manifest:
                contenido_manifest = archivo_manifest.read()

            #with open((self.directorio_json / archivo.name), "w", encoding="utf-8") as manifest_final:
            #    manifest_final.write(self._formatear_archivo_manifest(contenido_manifest))

            manifest_data = list(map(self._add_line_break, contenido_manifest.split(" ")))
            formatted_file = "".join([str(x.data) for x in manifest_data])
            return StandardResponse(
                success=True,
                data=formatted_file,
                message="Archivo manifest formateado correctamente."
            )

        except FileNotFoundError as e:
            return StandardResponse(
                success=False,
                message=f"Archivo no encontrado: '{manifest_file}'",
                error_details=str(e)
            )
        except Exception as e:
            return StandardResponse(
                success=False,
                message=f"Error al procesar el archivo '{manifest_file.name}'.",
                error_details=str(e)
            )

    def _merge_dicts_recursive(self, dict1, dict2):
        """
        Fusiona dos diccionarios de forma recursiva, actualizando valores existentes
        y agregando nuevos datos.

        Parameters:
            dict1 (dict): Primer diccionario (base).
            dict2 (dict): Segundo diccionario (datos nuevos).

        Returns:
            dict: Diccionario fusionado.
        """
        for key, value in dict2.items():
            if key in dict1 and isinstance(dict1[key], Mapping) and isinstance(value, Mapping):
                self._merge_dicts_recursive(dict1[key], value)
            else:
                dict1[key] = value
        return dict1

    def build_list_old(self, folder_path: Path) -> StandardResponse:
        """
        Construye la lista de archivos .DATA a procesar.

        Parameters:
            folder_path (Path): Directorio con los archivos a clasificar.

        Returns:
            StandardResponse: Clase estandar para encapsular respuestas de funciones.
        """

        resultados  = []
        parejas_dict = self._data_file_matching(folder_path).data

        if isinstance(parejas_dict, dict):
            for par in parejas_dict.values():
                if par["complemento"]:
                    resultados.append(par["complemento"])
                elif par["original"]:
                    resultados.append(par["original"])

        manifest = list(folder_path.glob("*.manifest"))
        return StandardResponse(
            success=True,
            data=resultados + manifest,
            message="Lista con archivos .DATA contruida correctamente."
        )

    def build_list(self, folder_path: Path) -> StandardResponse:
        """
        Construye la lista de archivos .DATA a procesar.

        Parameters:
            folder_path (Path): Directorio con los archivos a clasificar.

        Returns:
            StandardResponse: Clase estandar para encapsular respuestas de funciones.
        """
        resultados = []
        parejas_dict = self._data_file_matching(folder_path).data

        if isinstance(parejas_dict, dict):
            for par in parejas_dict.values():
                if par["complemento"] and par["original"]:
                    try:
                        with open(par["original"], "r", encoding="utf-8") as original_file:
                            original_content = xmltodict.parse(original_file.read())
                        with open(par["complemento"], "r", encoding="utf-8") as complemento_file:
                            complemento_content = xmltodict.parse(complemento_file.read())

                        fusion_content = self._merge_dicts_recursive(original_content, complemento_content)

                        fusion_xml = xmltodict.unparse(fusion_content, pretty=True)

                        fusion_file = folder_path / f"{par['original'].stem}_fusionado.DATA"
                        with open(fusion_file, "w", encoding="utf-8") as fusion_file_obj:
                            fusion_file_obj.write(fusion_xml)

                        resultados.append(fusion_file)
                    except Exception as e:
                        return StandardResponse(
                            success=False,
                            message=f"Error al fusionar archivos XML: {e}",
                            error_details=str(e)
                        )
                elif par["complemento"]:
                    resultados.append(par["complemento"])
                elif par["original"]:
                    resultados.append(par["original"])

        manifest = list(folder_path.glob("*.manifest"))
        return StandardResponse(
            success=True,
            data=resultados + manifest,
            message="Lista con archivos .DATA construida correctamente."
        )

    def _data_file_matching(self, folder_path: Path) -> StandardResponse:
        """
        Empareja los archivos .DATA (originales y complementos).

        Parameters:
            folder_path (Path): Directorio con los archivos a clasificar.

        Returns:
            StandardResponse: Clase estandar para encapsular respuestas de funciones.
        """
        parejas: Dict[str, Dict[str, Optional[Path]]] = {}

        for archivo_path in folder_path.glob("*.DATA"):
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

        return StandardResponse(
            success=True,
            data=parejas,
            message="Archivos .DATA emparejados correctamente."
        )

    def transform_xml_to_dict(self, xml_file: Path) -> StandardResponse:
        """
        Transforma el contenido de un archivo XML a Diccionario.

        Parameters:
            xml_file (Path): Archivo XML a transformar.

        Returns:
            StandardResponse: Clase estandar para encapsular respuestas de funciones.

        Raises:
            FileNotFoundError: Si el archivo no existe.
            xmltodict.ParsingInterrupted: Si el arhcivo XML no tiene un formato valido.
            Exception: Si ocurre un error al transformar un archivo.
        """

        try:
            if not xml_file.suffix.lower() == ".data":
                return StandardResponse(
                    success=False,
                    message=f"El archivo '{xml_file.name}' no tiene la extension .DATA"
                )

            with open(xml_file, 'r', encoding="utf-8") as archivo_xml:
                xml_contenido = archivo_xml.read()

            xml_dict = xmltodict.parse(xml_contenido)
            #json_data = json.dumps(xml_dict, indent=4)
            dict_data = json.dumps(xml_dict)
            #archivo_json_name = archivo.with_suffix(".json").name

            #with open((self.directorio_json / archivo_json_name), 'w', encoding="utf-8") as archivo_json:
            #    archivo_json.write(json_data)

            return StandardResponse(
                success=True,
                data=dict_data,
                message=f"Contenido XML del archivo '{xml_file.name}' transformado a diccionario correctamente."
            )

        except FileNotFoundError as e:
            return StandardResponse(
                success=False,
                message=f"Archivo no encontrado: '{xml_file.name}'.",
                error_details=str(e)
            )
        except xmltodict.ParsingInterrupted as e:
            return StandardResponse(
                success=False,
                message=f"Error al parsear XML en '{xml_file.name}': {e}.",
                error_details=str(e)
            )
        except Exception as e:
            return StandardResponse(
                success=False,
                message=f"Error al procesar el archivo '{xml_file.name}'.",
                error_details=str(e)
            )
