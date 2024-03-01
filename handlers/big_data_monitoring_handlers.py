from config import bot
from operations.main_logic_checkup import random_row_start
from aiogram import Dispatcher
from aiogram import types

models_from_delta_list = []
async def random_edw_and_bre_row_monitorng(msg: types.Message):
    response_message = random_row_start(models_from_delta_list)
    await bot.send_message(CHAT_ID, response_message)


def register_big_data_models_monitoring_handlers(dp: Dispatcher) -> None:
    dp.register_message_handler(random_edw_and_bre_row_monitorng, commands='big_data_monitoring_start')
