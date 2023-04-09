import requests
from . import credentials
from . import endpoints
import json
import os
import sys
import time
sys.path.append('D:/one1/projects/data_pipeline/tut_lesson_to_mysql')
from database.push_data import push_data_in_destination
from database.database_func import get_id_from_contractor, get_id_from_contractor_all

def check_url_update():
    print(endpoints.urls)
    for url in endpoints.urls:
        print("---")
        print(url)
        print(endpoints.urls[url])
        endpoints.urls[url] = "previous"
        print(endpoints.urls[url])

       
        endpoints.urls[url] = 'https://example.com/'+ url 
    
        # Write the updated JSON object back to the file
        with open('endpoints.py', 'w') as f:
            json.dump(endpoints.urls, f)

def tut_fetch_all_data():

    for url in endpoints.urls:
        print(endpoints.urls[url])

        table_name = url
       
        is_next_page = True

        page_url = endpoints.urls[url]
        while is_next_page:

            print("Calling page: {}".format(page_url))
            response = requests.request("GET", page_url, headers=credentials.headers)
            data = json.loads(response.text)
          

            if data["next"] == None:
                
                is_next_page = False
                endpoints.urls[url] = data["previous"]
                
            else:
                page_url = data["next"]

            push_data_in_destination(data, table_name)



