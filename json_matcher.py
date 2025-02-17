from typing import Dict, Any

class JsonMatcher:
    """
    Clase para agregar descripcion a un archivo JSON basado en un diccionario de descripciones.
    """
    
    def __init__(self) -> None:
        pass

    def agregar_descripcion_json(self,
                                data_json: Dict[str, Any],
                                descripciones: Dict[str, str],
                                suffix: str = "_description") -> Dict[str, str]:
        """
        Agrega descripciones a un JSON basado en un diccionario de descripciones.
        
        Args:
            data_json (Dict[str,Any]): JSON al que le agregaran las descripciones.
            descripciones (Dict[str, str]): Diccionario de descripciones.
            suffix (str): Sufijo que se agregara a las nuevas claves que contienen las descripciones.
        
        Returns:
            Dict[str,Any]: JSON con las descripciones agregadas.
        
        Raises:
            TypeError: Si los parametros son de tipo incorrecto.
        """
        if not isinstance(descripciones, dict):
            raise TypeError("'descripciones' debe ser un diccionario.")
        
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
                nueva_data_json[key] = self.agregar_descripcion_json(value, descripciones, suffix)
                
                if isinstance(value, str) and value in descripciones:
                    nueva_key = f"{key}{suffix}"
                    nueva_data_json[nueva_key] = descripciones[value]
            return nueva_data_json
        elif isinstance(data_json, list):
            return [self.agregar_descripcion_json(item, descripciones, suffix) for item in data_json]
        else:
            return data_json