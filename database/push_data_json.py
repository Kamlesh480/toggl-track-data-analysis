import sqlalchemy as db
import json
import os
from . import sql_query_generator
from . import database_func
from datetime import datetime


current_time = datetime.fromtimestamp(int(datetime.now().timestamp())).strftime('%Y-%m-%d %H:%M:%S')
# column_values['insert_time'] = current_time

# current_time = int(datetime.now().timestamp())
print("Current time:", str(current_time))

json_path = 'tutorcruncher/test_push_data/'
json_files = [f for f in os.listdir(json_path) if f.endswith('.json')]
print(json_files)

def push_data_in_destination(prifix):
    # count = 0
    for json_file in json_files:
        with open(os.path.join(json_path, json_file)) as f:
            data = json.load(f)
            # print(json_file)
            print("\n")
            # print(data)
            
            # if count >= 1:
            #     break
            # count += 1
             
            table_name = prifix + json_file.split('.json')[0]
            print(table_name)
            table_name = f"`{table_name}`"
            print(table_name)

            if data:
                if isinstance(data, list) and len(data) > 0:
                    for d in data:
                        d["insert_time"] = current_time
                        print(d)
                        database_func.insert_value(d,table_name)
                    # data = data[0]

                if isinstance(data, dict) and "results" in data:
                    for d in data["results"]:
                        # d = {'id': 1133654, 'first_name': 'Kamlesh', 'last_name': 'Moldavsky', 'email': 'alexmoldster@gmail.com', 'url': 'https://secure.tutorcruncher.com/api/clients/1133654/', 'insert_time': '2023-03-03 17:53:03'}
                        d["insert_time"] = current_time
                        print(d)
                        database_func.insert_value(d,table_name)
                    else:
                        pass

                if data:
                    # query = sql_query_generator.generate_table_query_from_json(data, table_name)
                    # print("data is {}".format(data))
                    print("data is uploaded for Table: {}".format(table_name))
                    # column_values = {
                    #         'created_time': str(current_time),
                    #         'sql_query': query,
                    #         'table_name': table_name
                    # }
                    # print("column_values is {}".format(column_values))
                    # database_func.insert_value(column_values, 'mapping_metadata')
                    # yield query




# push_data_in_destination("sb_")

# query = create_table_stored_json()
# print(query)
# database_func.create_table(query)