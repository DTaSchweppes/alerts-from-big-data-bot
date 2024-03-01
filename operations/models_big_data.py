from sql.bigdata_models_queries import SQL_MODELS_LIST_QUERY, SQL_RANDOM_BIGDATA_MODELS_QUERY, \
    SQL_MODELS_REPORT_DATE_LIST_QUERY
from sql.busines_schema import TABLE_NAME
from api_bre import URL


class ModelsBigData:

    def __init__(self):
        self.models_list = self.parse_models_list()
        self.models_report_date_list = self.get_models_and_report_date_list()
        self.count_errors = 0 #количество ошибок когда модели big data не совпадают в EDW и BRE
        self.count_all_models = 0
        self.random_row_with_one_subs_id_and_count_his_bigdata_models = [] #рандомная строка из EDW один id и количество моделей у него
        self.count_bigdata_models_in_one_subs_id = 0 # количество моделей у одного subs_id
        self.subs_id_from_row_count_models_and_one_subs_id = None #айдишник из строки с количеством моделей
        self.list_of_subs_id_models = []
        self.delta_edw_and_bre = [] # разница в количестве данных в EDW и BRE

    def get_models_list(self) -> list:
        with td_tools.teradata_connect() as td_conn:
            session = td_conn.cursor()
            result = session.execute(SQL_MODELS_LIST_QUERY)  # выполнение запроса
            return result.fetchall()

    def parse_models_list(self):
        source_models_list = self.get_models_list()
        destination_list_with_in_values = []
        for model_list_type in source_models_list:
            destination_list_with_in_values.append(int(model_list_type[0]))
        return destination_list_with_in_values

    def check_report_date_to_current_date(self):
        msg = ''
        logging.info(f' {self.models_report_date_list} models_report_date_list стал заполнен после запроса ')
        self.count_all_models = len(self.models_report_date_list)
        for model in self.models_report_date_list:
            if model[1] != datetime.date.today():
                msg += f'Report date у модели {model[0]} в EDW не совпадает с текущей {datetime.date.today()} датой\n'
                self.count_errors += 1
                logging.info('count errors')
        return msg

    def get_random_row_twenty_bigdata_models_with_subs_id_frow_edw(self):
        time_after = time.time()
        with td_tools.teradata_connect() as td_conn:
            session = td_conn.cursor()
            result = session.execute(SQL_RANDOM_BIGDATA_MODELS_QUERY)  # выполнение запроса
            fetchall_result = result.fetchall()
            logging.info(f' рандомная запись одна {fetchall_result}')
            time_before = time.time()
            execution_time = time_before - time_after
            logging.info(f'Время отработки запроса рандом записи EDW >20 big_data_models: {execution_time}')
            return fetchall_result

    def get_models_and_report_date_list(self):
        time_after = time.time()
        with td_tools.teradata_connect() as td_conn:
            session = td_conn.cursor()
            result = session.execute(SQL_MODELS_REPORT_DATE_LIST_QUERY)  # выполнение запроса
            time_before = time.time()
            execution_time = time_before - time_after
            logging.info(f'Время отработки запроса на Report date моделей: {execution_time}')
            return result.fetchall()

    def get_subs_id_from_random_row_edw(self):
        self.random_row_with_one_subs_id_and_count_his_bigdata_models = self.get_random_row_twenty_bigdata_models_with_subs_id_frow_edw()
        logging.info(f' self.random_row_subs_id_bigdata_models {self.random_row_with_one_subs_id_and_count_his_bigdata_models}')
        self.count_bigdata_models_in_one_subs_id = self.random_row_with_one_subs_id_and_count_his_bigdata_models[0][0]
        self.subs_id_from_row_count_models_and_one_subs_id = self.random_row_with_one_subs_id_and_count_his_bigdata_models[0][1]

    def send_post_with_subs_id_to_bre(self):
        logging.info(f'отправка в БРЕ ключ {int(self.subs_id_from_row_count_models_and_one_subs_id)}')
        response = requests.post(url=URL,
                                 json={"requests": [
                                     {"cache": "subs_score_current", "key": int(self.subs_id_from_row_count_models_and_one_subs_id)}]})
        return json.loads(response.content)

    def send_subs_id_from_edw_to_bre_for_check(self):
        self.get_subs_id_from_random_row_edw()
        return self.send_post_with_subs_id_to_bre()

    def check_largest_models_in_edw_than_bre(self, list_from_edw: list, list_from_bre: list):
        if len(list_from_edw) > len(list_from_bre):
            return True

    def get_delta_lists_models_bre_edw(self, list_models_from_key_bre: list):
        self.get_list_models_for_subs_id_in_edw()
        set1 = set(self.list_of_subs_id_models)  # чтобы он заполнился надо запустить
        set2 = set(list_models_from_key_bre)
        edw_not_in_bre = sorted(list(set1 - set2))
        bre_not_in_edw = sorted(list(set2 - set1))
        if self.check_largest_models_in_edw_than_bre(self.models_list, list_models_from_key_bre):
            self.delta_edw_and_bre = edw_not_in_bre
        else:
            self.delta_edw_and_bre = edw_not_in_bre

    def get_list_models_for_subs_id_in_edw(self):
        SQL_MODELS_OF_SUBS = f'select distinct model_id from {TABLE_NAME} where subs_id={self.subs_id_from_row_count_models_and_one_subs_id}'
        with td_tools.teradata_connect() as td_conn:
            session = td_conn.cursor()
            result = session.execute(SQL_MODELS_OF_SUBS)  # выполнение запроса
            fetchall_result = result.fetchall()
        original_list = fetchall_result
        flattened_list = [item for sublist in original_list for item in sublist]
        self.list_of_subs_id_models = flattened_list


    def start_check_edw_bre(self, count_models_bre: int, list_models_from_key_bre: list):
        SQL_COUNT_MODELS = f'select count(distinct model_id) from {TABLE_NAME} where subs_id={int(self.subs_id_from_row_count_models_and_one_subs_id)}'
        with td_tools.teradata_connect() as td_conn:
            session = td_conn.cursor()
            result = session.execute(SQL_COUNT_MODELS)  # выполнение запроса
            fetchall_result = result.fetchall()
        if count_models_bre == fetchall_result[0][0]:
            logging.info(f'Количество моделей у subs_id {int(self.subs_id_from_row_count_models_and_one_subs_id)} в EDW и BRE совпадает')
        else:
            logging.info(f'Количество моделей у subs_id {int(self.subs_id_from_row_count_models_and_one_subs_id)} в EDW и BRE не совпадает')
            self.get_delta_lists_models_bre_edw(list_models_from_key_bre)