import json


class ModelFromBRERequest:
    '''
    Описание модели полученной из API
    '''
    def __init__(self, json_from_bre: dict):
        self.json_response = json_from_bre
        self.count_models_from_key_bre = 0
        self.list_models_from_key_bre = []

    def parse_json(self):
        responses = self.json_response['responses']
        parsing_step = json.loads(responses[0]['attr_list']['jsonval'])
        for i in parsing_step:
            self.list_models_from_key_bre.append(i['model'])
        self.count_models_from_key_bre = len(set(self.list_models_from_key_bre))