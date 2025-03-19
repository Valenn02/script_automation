from typing import Dict, Any

class JsonMatcher:
    """
    Clase para agregar descripcion a un archivo JSON basado en un diccionario de descripciones.
    """

    def __init__(self) -> None:
        pass

    def add_description_json(self,
                            data_json: Dict[str, Any],
                            descriptions: Dict[str, str],
                            suffix: str = "_description"
                            ) -> Dict[str, str]:
        """
        Agrega descripciones a un JSON basado en un diccionario de descripciones.

        Parameters:
            data_json (Dict[str,Any]): JSON al que le agregaran las descripciones.
            descriptions (Dict[str, str]): Diccionario de descripciones.
            suffix (str): Sufijo que se agregara a las nuevas claves que contienen las descripciones.

        Returns:
            Dict[str,Any]: JSON con las descripciones agregadas.
        """

        #if isinstance(data_json, dict):
        #    for key in list(data_json.keys()):
        #        value = data_json[key]
        #        data_json[key] = self.agregar_descripcion_json(value, descripciones, suffix)

        #       if isinstance(data_json[key], str) and data_json[key] in descripciones:
        #            new_key = f"{key}{suffix}"
        #            data_json[new_key] = descripciones[data_json[key]]
        #    return data_json
        #elif isinstance(data_json, list):
        #    return [self.agregar_descripcion_json(item, descripciones, suffix) for item in data_json]
        #else:
        #    return data_json

        if isinstance(data_json, dict):
            nueva_data_json: Dict[str, Any] = {}
            for key, value in data_json.items():
                nueva_data_json[key] = self.add_description_json(value, descriptions, suffix)

                if isinstance(value, str) and value in descriptions:
                    nueva_key = f"{key}{suffix}"
                    nueva_data_json[nueva_key] = descriptions[value]
            return nueva_data_json
        elif isinstance(data_json, list):
            return [self.add_description_json(item, descriptions, suffix) for item in data_json]
        else:
            return data_json
