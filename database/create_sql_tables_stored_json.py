import sqlalchemy as db
import json
import os
from . import sql_query_generator
from . import database_func
from datetime import datetime

current_time = datetime.fromtimestamp(int(datetime.now().timestamp())).strftime('%Y-%m-%d %H:%M:%S')

json_path = 'tutorcruncher/imp_fetched_data/'
json_files = [f for f in os.listdir(json_path) if f.endswith('.json')]
print(json_files)

def create_table_stored_json(prifix):
    # count = 0
    for json_file in json_files:
        with open(os.path.join(json_path, json_file)) as f:
            data = json.load(f)
            print(json_file)
            
            # if count > 1:
            #     break
            # count += 1
             
            table_name = json_file.split('.json')[0]
            table_name = prifix + table_name
            print(table_name)

            if data:
                if isinstance(data, list) and len(data) > 0:
                    data = data[0]

                if isinstance(data, dict) and "results" in data:
                    if len(data["results"]) > 0:
                        data = data["results"][0]
                    else:
                        pass

                if data:
                    query = sql_query_generator.generate_table_query_from_json(data, table_name)
                    print("query is created {}".format(query))
                    column_values = {
                            'created_time': str(current_time),
                            'sql_query': query,
                            'table_name': table_name
                    }
                    # print("column_values is {}".format(column_values))
                    database_func.old_insert_value(column_values, 'mapping_metadata')
                    yield query



def run_create_temp_table_stored_json(prifix):

    for query in create_table_stored_json(prifix):
        database_func.create_table(query)
        # print("Done _____________ table created")

# run_create_temp_table_stored_json("")

def run_create_main_table_stored_json(prifix):
    for query in create_table_stored_json(prifix):
        database_func.create_table(query)
        # print("Done _____________ table created")

# run_create_main_table_stored_json("sb_")






# query = create_table_stored_json()
# print(query)
# database_func.create_table(query)