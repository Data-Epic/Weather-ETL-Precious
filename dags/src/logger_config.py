import logging
import os


def setup_loggers():
    log_directory = "./logs/app_logs"
    os.makedirs(log_directory, exist_ok=True)

    log_file = os.path.join(log_directory, "weather_etl.log")
    error_log_file = os.path.join(log_directory, "weather_etl-error.log")

    formatter = logging.Formatter("%(asctime)s - %(message)s")

    # Setup info logger
    info_logger = logging.getLogger("info_logger")
    info_logger.setLevel(logging.INFO)
    if not info_logger.handlers:
        info_handler = logging.FileHandler(log_file)
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(formatter)
        info_logger.addHandler(info_handler)

    # Setup error logger
    error_logger = logging.getLogger("error_logger")
    error_logger.setLevel(logging.ERROR)
    if not error_logger.handlers:
        error_handler = logging.FileHandler(error_log_file)
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        error_logger.addHandler(error_handler)

    return info_logger, error_logger


info_logger, error_logger = setup_loggers()
