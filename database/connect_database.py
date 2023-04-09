import pymysql
from . import credentials


password = credentials.password
username = credentials.username
database = credentials.database
host = credentials.host


def conn():
    # database connection
    connection = pymysql.connect(host=host, port=3306, user=username, passwd=password, database=database)
    return connection


# conn()