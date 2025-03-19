from pathlib import Path
from datetime import datetime
from standard_response import StandardResponse

class MetadataExtractor:
    """
    Clase para extraer metadatos de archivos TAR.
    """

    def __init__(self) -> None:
        pass

    def metadata_extractor(self, file: Path) -> StandardResponse:
        """
        Extrae los metadatos del archivo.

        Parameters:
            file (Path): Ruta del archivo del cual se extraeran los metadatos.

        Returns:
            StandardResponse: Clase estandar para encapsular respuestas de funciones.

        Raises:
            OSError: Si el archivo no puede ser leido.
            Exception: Si ocurre un error inesperado al extraer los metadatos.
        """
        try:
            if not file.exists():
                return StandardResponse(
                    success=False,
                    message=f"El archivo {file.name} no existe."
                )

            if not file.is_file():
                return StandardResponse(
                    success=False,
                    message=f"'{file.name}' no es un archivo."
                )

            metadatos = {
                "nombre_archivo": file.name,
                "tamanio_archivo": file.stat().st_size,
                "fecha_creacion": datetime.fromtimestamp(file.stat().st_ctime).strftime("%Y-%m-%d %H:%M:%S"),
                "fecha_modificacion": datetime.fromtimestamp(file.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "tipo_archivo": file.suffix
            }
            return StandardResponse(
                success=True,
                data=metadatos,
                message=f"Metadatos extraidos de '{file.name}' correctamente."
            )

        except OSError as e:
            return StandardResponse(
                success=False,
                message=f"Error al extraer metadatos del archivo '{file.name}'.",
                error_details=str(e)
            )
        except Exception as e:
            return StandardResponse(
                success=False,
                message=f"Error inesperado al extraer metadatos del archivo '{file.name}'.",
                error_details=str(e)
            )
