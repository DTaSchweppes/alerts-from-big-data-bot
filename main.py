import logging
import os

from aiogram.utils import executor
from config import dp
from aiogram import types
from handlers.jobs_handlers import register_jobs_handlers
from scheduler.scheduler import sheduler

logging_level = logging.INFO
logging.basicConfig(level=logging_level,
                    format="%(asctime)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    handlers=[
                        logging.FileHandler(f'{os.path.basename(__file__)}.log'),
                        logging.StreamHandler()
                    ]
                    )

sheduler.start()
register_jobs_handlers(dp)


if __name__ == "__main__":
    executor.start_polling(dp)