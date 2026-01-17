# message_bus.py
import logging
from datetime import datetime

class MessageBus:
    def __init__(self):
        self.logs = []
        self._setup_logging()

    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger("MarketRadar")

    def publish(self, event_type, message, status=True, error=None):
        """
        Publish an event/log.
        :param event_type: "INFO", "ERROR", "WARNING", "DATA_FETCH"
        :param message: Description
        :param status: True (Pass) / False (Fail) - main used for summaries
        :param error: Error object or string if any
        """
        log_entry = {
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "type": event_type,
            "message": message,
            "status": status,
            "error": str(error) if error else None
        }
        self.logs.append(log_entry)
        
        if event_type == "ERROR" or not status:
            self.logger.error(f"{message} | Error: {error}")
        else:
            self.logger.info(message)

    def get_logs(self):
        return self.logs

    def get_summary(self):
        success = sum(1 for l in self.logs if l['status'])
        fails = sum(1 for l in self.logs if not l['status'])
        return f"Total: {len(self.logs)} | Success: {success} | Failed: {fails}"
