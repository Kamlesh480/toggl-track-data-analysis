import requests
from base64 import b64encode

json_data = {"user_ids": [ 8877764], "start_date": "2022-10-10"}
headers = {'content-type': 'application/json', 'Authorization' : 'Basic %s' %  b64encode(b"kamleshchhipa480@gmail.com:Toggle@4321#").decode("ascii")}
response = requests.post('https://api.track.toggl.com/reports/api/v3/workspace/6775596/search/time_entries', json=json_data, headers=headers)

print(response.json())
