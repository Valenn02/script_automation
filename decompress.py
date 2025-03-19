from pathlib import Path
from standard_response import StandardResponse
import tarfile
import logging

class TarDecompressor:
    """
    Clase para descomprimir un archivo TAR en un directorio especifico.

    Attributes:
        directorio_destino (Path): Directorio para almacenar el contenido del archivo TAR.
    """

    def __init__(self, directorio_destino: Path) -> None:
        """
        Constructor para la clase TarDecompressor.

        Parameters:
            directorio_destino (Path): Directorio para almacenar el contenido del archivo TAR.
        """
        self.directorio_destino = directorio_destino

    def decompress_tar_gz(self, archivo_tar_gz: Path) -> StandardResponse:
        """
        Descomprime un archivo TAR en un directorio especifico.

        Parameters:
            archivo_tar_gz (Path): Archivo TAR para descomprimir.

        Returns:
            StandardResponse: Clase estandar para encapsular respuestas de funciones.

        Raises:
            FileNotFoundError: Si el archivo no existe.
            TarError: Si el archivo no es un archivo TAR.
            Exception: Si ocurre algun error inesperado.
        """
        try:
            if not archivo_tar_gz.exists():
                return StandardResponse(
                    success=False,
                    message=f"El archivo '{archivo_tar_gz.name}' no existe."
                )

            if not tarfile.is_tarfile(archivo_tar_gz):
                return StandardResponse(
                    success=False,
                    message=f"El archivo '{archivo_tar_gz.name}' no es un archivo TAR valido."
                )

            directorio_descompresion = Path(self.directorio_destino / archivo_tar_gz.name)
            directorio_descompresion.mkdir(parents=True, exist_ok=True)

            with tarfile.open(archivo_tar_gz, "r:gz") as archivo_tar:
                archivo_tar.extractall(path=directorio_descompresion)

            return StandardResponse(
                success=True,
                message=f"El archivo '{archivo_tar_gz.name}' se descomprimio correctamente en {directorio_descompresion}"
            )
        except (FileNotFoundError, tarfile.TarError, Exception) as e:
            return StandardResponse(
                success=False,
                message=f"Error al descomprimir '{archivo_tar_gz.name}'.",
                error_details=str(e)
            )
