from sql.busines_schema import TABLE_NAME, BRANCH_NAME

SQL_MODELS_LIST_QUERY = f'select distinct model_id from {TABLE_NAME}'

SQL_MODELS_REPORT_DATE_LIST_QUERY = f'select distinct model_id,report_date from {TABLE_NAME}'

SQL_RANDOM_BIGDATA_MODELS_QUERY = f"select count(model_id) as count_models,subs_id from {TABLE_NAME} where branch_name='{BRANCH_NAME}' GROUP BY subs_id HAVING COUNT(model_id) > 19 SAMPLE RANDOMIZED ALLOCATION 1"

