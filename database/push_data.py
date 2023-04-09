import sqlalchemy as db
import json
import os
from . import sql_query_generator
from . import database_func
from datetime import datetime


current_time = datetime.fromtimestamp(int(datetime.now().timestamp())).strftime('%Y-%m-%d %H:%M:%S')

json_path = 'tutorcruncher/test_push_data/'
json_files = [f for f in os.listdir(json_path) if f.endswith('.json')]
print(json_files)

def push_data_in_destination(data, table_name, **kwargs):
    # count = 0
    if isinstance(data, dict) and "results" in data:
        for d in data["results"]:
            d["insert_time"] = current_time
            # print(d)
            database_func.insert_value(d,table_name)

    elif isinstance(data, list):
        for d in data:
            # d["insert_time"] = current_time
            d["contractor_id"] = kwargs.get('c_id')
            d["contractor_insert_time"] = kwargs.get('c_time')
            database_func.insert_value(d, table_name)
        print("Data uploaded for Table: {}".format(table_name))

    else:
        pass

    if data:
        print("data is uploaded for Table: {}".format(table_name))




# push_data_in_destination("sb_")