import sql_query_generator

def create_table(data, table_name):
    
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
            return query
                    

create_table( {},'`clients`')