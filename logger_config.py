from log_db_handler import MongoLoggingDBHandler
from dotenv import load_dotenv
import logging
import sys
import os

load_dotenv(override=True)
def configuracion_logging(level=logging.INFO) -> None:
    """Configuracion del logging para toda la aplicacion"""

    mongo_handler = MongoLoggingDBHandler(
        mongo_uri=os.getenv("CONNECTION_URL_MONGO", ""),
        db_name=os.getenv("DB_NAME", "BOA_VUELOS"),
        collection_logs="LOGS_PROCESADOS",
        buffer_size=int(os.getenv("BUFFER_SIZE_LOGS", 40)),
        waiting_time=float(os.getenv("WAITING_TIME", 1)),
        max_attempts=int(os.getenv("MAX_ATTEMPTS", 3))
    )

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout),
            #logging.FileHandler("automation.log", mode="w"),
            logging.FileHandler("automation.log", mode="a", encoding="utf-8"),
            mongo_handler
        ]
    )
