from apscheduler.schedulers.asyncio import AsyncIOScheduler
from handlers.big_data_monitoring_handlers import random_edw_and_bre_row_monitorng

sheduler = AsyncIOScheduler(timezone='Europe/Moscow')

sheduler.add_job(random_edw_and_bre_row_monitorng, trigger='cron', hour=14,
                 minute=10, kwargs={"msg": None})