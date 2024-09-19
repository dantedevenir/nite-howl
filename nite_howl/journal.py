import logging
from datetime import datetime

class Minute:
    
    def __init__(self) -> None:
        current_date = datetime.now().strftime("%B %dth, %Y")  # Obtener la fecha actual
        # Configurar logging con marca de tiempo y el mensaje "Rorschach's journal"
        logging.basicConfig(
            level=logging.INFO,
            format=f"Rorschach's journal: {current_date}, %(asctime)s - %(message)s",
            datefmt="%H:%M:%S",  # Formato personalizado para la marca de tiempo
        )
        self.logger = logging.getLogger()
        
    def register(self, type, text) -> None:
        if type == "error":
            self.logger.error(text)
        elif type == "warning":
            self.logger.warning(text)
        elif type == "debug":
            self.logger.debug(text)
        else:
            self.logger.info(text)
minute = Minute()