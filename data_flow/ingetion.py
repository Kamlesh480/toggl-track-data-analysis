import sys
sys.path.append('D:/one1/projects/data_pipeline/tut_lesson_to_mysql')


from toggle_tracker.tut_fetch_all_objects import tut_fetch_all_data
from thelessonspace.lesson_fetch_all_objects import lesson_fetch_all_data


def fetch_from_tut():
    print("getting data for tut")
    for data in tut_fetch_all_data():
        print("Data fetched:", data)

fetch_from_tut()
        



# print("getting data for lesson")
# for data in lesson_fetch_all_data():
#     print("Data fetched:", data)