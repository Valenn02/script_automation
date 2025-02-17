from pathlib import Path
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
        Inicializa el descompresor.
        
        Args:
            directorio_destino: Directorio donde se extraeran los archivos.
        """
        self.directorio_destino = directorio_destino

    def descomprimir_tar_gz(self, archivo_tar_gz: Path) -> None:
        """
        Descomprime un archivo TAR en un directorio especifico.
        
        Args:
            archivo_tar_gz (Path): Archivo TAR para descomprimir.
        
        Returns:
            bool: True si la operacion fue exitosa, False en caso contrario.
        
        Raises:
            FileNotFoundError: Si el archivo no existe.
            TarError: Si el archivo no es un archivo TAR.
            Exception: Si ocurre algun error inesperado.
        """
        try:
            if not archivo_tar_gz.exists():
                raise FileNotFoundError(f"El archivo '{archivo_tar_gz.name}' no existe.")

            if not tarfile.is_tarfile(archivo_tar_gz):
                raise tarfile.TarError(f"El archivo '{archivo_tar_gz.name}' no es un archivo TAR valido.")

            directorio_descompresion = Path(self.directorio_destino / archivo_tar_gz.name)
            directorio_descompresion.mkdir(parents=True, exist_ok=True)

            with tarfile.open(archivo_tar_gz, "r:gz") as archivo_tar:
                archivo_tar.extractall(path=directorio_descompresion)

            logging.info(f"El archivo '{archivo_tar_gz.name}' se descomprimio correctamente en {directorio_descompresion}")
        except (FileNotFoundError, tarfile.TarError, Exception) as e:
            logging.error(f"Error al descomprimir '{archivo_tar_gz.name}': {e}")
            raise