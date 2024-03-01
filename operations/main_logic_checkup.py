from operations.bigdata_model_from_bre import ModelFromBRERequest
from operations.models_big_data import ModelsBigData


def check_one_random_bigdata_model(models_from_delta_list: list) -> str:
    models_manager = ModelsBigData()
    request_from_bre = ModelFromBRERequest(json_from_bre=models_manager.send_subs_id_from_edw_to_bre_for_check())
    request_from_bre.parse_json()
    models_manager.start_check_edw_bre(request_from_bre.count_models_from_key_bre,
                                       request_from_bre.list_models_from_key_bre)
    models_from_delta_list.extend(models_manager.delta_edw_and_bre)
    return f'У subs_id {models_manager.subs_id_from_row_count_models_and_one_subs_id} дельта моделей между EDW и BRE = {len(models_manager.delta_edw_and_bre)} шт. \n'


def random_row_start(models_from_delta_list: list) -> str:
    msg = ''
    for i in range(0, 2):
        msg += check_one_random_bigdata_model(models_from_delta_list)
    msg += (f'В ходе 2ух проверок рандомных записей в EDW по subs_id нашли модели, которых нет у тех же subs_id в '
            f'BRE⛔: \n{set(sorted(models_from_delta_list))}\nИтого после сканирования двух рандомных записей, '
            f'выявлено отстутствие {len(set(models_from_delta_list))} моделей в BRE❗\n')
    return msg
