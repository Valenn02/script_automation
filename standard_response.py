from dataclasses import dataclass
from typing import Optional, Any

@dataclass
class StandardResponse:
    """
    Clase estandar para encapsular respuestas de funciones:

    Attributes:
        success (Optional[bool]): Indica si la operacion fue exitosa.
        data (Optional[Any]): Los datos retornados de la operacion (default=None).
        message (Optional[str]): Un mensaje descriptivo sobre la informacion (default=None).
        error_details (Optional[str]): Detalles de error (default=None).
    """
    success: bool
    data: Optional[Any]=None
    message: Optional[str]=None
    error_details: Optional[str]=None
