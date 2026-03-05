import logging
from logging.handlers import RotatingFileHandler
import os

# -------------------------------------------------------------------
# Configure environment-based logging directory
# -------------------------------------------------------------------
ENV = os.getenv("ENVIRONMENT", "production")
LOG_DIR = os.path.join("logs", ENV)
os.makedirs(LOG_DIR, exist_ok=True)

# Define a common log format
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(LOG_FORMAT)

# -------------------------------------------------------------------
# Global Handlers
# -------------------------------------------------------------------

# Global server file handler
server_file_handler = RotatingFileHandler(
    os.path.join(LOG_DIR, "webhook.log"),
    maxBytes=50 * 1024 * 1024,  # 50 MB
    backupCount=7
)
server_file_handler.setLevel(logging.DEBUG)
server_file_handler.setFormatter(formatter)

# Global error file handler
error_file_handler = RotatingFileHandler(
    os.path.join(LOG_DIR, "error.log"),
    maxBytes=50 * 1024 * 1024,
    backupCount=7
)
error_file_handler.setLevel(logging.ERROR)
error_file_handler.setFormatter(formatter)


# -------------------------------------------------------------------
# Logger Setup
# -------------------------------------------------------------------

def setup_root_logger():
    """
    Sets up the root logger used by webhook managers.
    Writes to webhook.log, error.log, and console.
    """
    logger = logging.getLogger("webhook")
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File handlers
        logger.addHandler(server_file_handler)
        logger.addHandler(error_file_handler)
    return logger


# -------------------------------------------------------------------
# Initialize Loggers
# -------------------------------------------------------------------

root_logger = setup_root_logger()
root_logger.info("Webhook log_config initialized.")
