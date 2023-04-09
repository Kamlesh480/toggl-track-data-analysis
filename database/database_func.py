import re
from pymysql import OperationalError
from . import connect_database
from . import credentials
import json
import time
import sys

connection = connect_database.conn()
cur = connection.cursor()
print(connection)

# def create_table(query, table_name):
#     cur.execute("SHOW TABLES LIKE %s", (table_name,))
#     result = cur.fetchone()

#     if result is None:
#         cur.execute(query)
#         connection.commit()
#         print(f"Table '{table_name}' created")
#     else:
#         print(f"Table '{table_name}' already exists")


def create_table(query):
    # global connection
    # global cur

    table_name = re.search(r"CREATE TABLE (\w+)", query).group(1)
    cur.execute(f"SHOW TABLES LIKE '{table_name}'")
    result = cur.fetchone()

    if result:
        print(f"Table {table_name} already exists")
        return

    cur.execute(query)
    connection.commit()
    print(f"Table {table_name} created")



def old_create_table(query):

    cur.execute(query)
    connection.commit()

    # Close the cursor and connection
    # cur.close()
    # connection.close()




def is_table_exists(table_name):

    result = cur.execute("SHOW TABLES LIKE %s", (table_name,))    
    connection.commit()
    # Close the database connection
    
    if result:
        return True
    else:
        return False





def old_insert_value(column_values, table):

    for key, value in column_values.items():
        if isinstance(value, (list, dict)):
            column_values[key] = json.dumps(value)

    # Build the SQL query
    # columns = ', '.join(column_values.keys())
    columns = ', '.join([f"`{key}`" for key in column_values.keys()])
    print(columns)
    # values = "', '".join(column_values.values())
    values = ', '.join(['%s'] * len(column_values))
    # print(values)
    insert_query = f"INSERT INTO {table} ({columns}) VALUES ({values})"
    print(insert_query)
    if 'subject_types' in column_values:
        column_values['subject_types'] = json.dumps(column_values['subject_types'])
    if 'space' in column_values:
        column_values['space'] = json.dumps(column_values['space'])
    # print(tuple(column_values.values()))
    cur.execute(insert_query, tuple(column_values.values()))

    # print(values)
    # insert_query = f"INSERT INTO {table} ({columns}) VALUES ('{values}')"
    # print(insert_query)

    # cur.execute(insert_query)
    connection.commit()

    # Close the database connection
    # cur.close()
    # connection.close()


def insert_value(column_values, table):
    global connection
    global cur

    for key, value in column_values.items():
        if isinstance(value, (list, dict)):
            column_values[key] = json.dumps(value)
    
    # Convert service field back to a string
    # if 'service' in column_values:
    #     column_values['service'] = json.dumps(column_values['service'])

    # print("column_values['service']: {}".format(column_values['service']))
    # print("type of column_values['service']:{}".format(type(column_values['service'])))
    # print("size:{}".format(sys.getsizeof(column_values['service'])))

    columns = ', '.join([f"`{key}`" for key in column_values.keys()])
    # print(columns)
    values = ', '.join(['%s'] * len(column_values))
    # print("values:{}".format(values))
    
    insert_query = f"INSERT INTO {table} ({columns}) VALUES ({values}) ON DUPLICATE KEY UPDATE "
    # update_query = ', '.join([f"`{key}` = %s" for key in column_values.keys() if key != 'id'])
    test_query = ', '.join([f"`{key}` = %s" for key in column_values.keys()])
    # print("column_values: {}".format(column_values.keys()))
    # print("update_query: {}".format(update_query))
    # print("test_query: {}".format(test_query))
    insert_query += test_query
    # print("insert_query: {}".format(insert_query))
    # print("first:{}".format(tuple(column_values.values())))
    # print("second:{}".format(tuple(column_values.values())[:-1]))
    # print("All:{}".format(tuple(column_values.values()) + tuple(column_values.values())[:-1]))

    # cur.execute(insert_query, tuple(column_values.values()) + tuple(column_values.values())[:-1])
    
    attempts = 0
    while attempts < 3: # retry at most 3 times
        try:
            cur.execute(insert_query, tuple(column_values.values()) + tuple(column_values.values()))
            connection.commit()
            print("added 1 row in {}".format(table))
            return
        except OperationalError:
            # reconnect to server
            connection = connect_database.conn()
            cur = connection.cursor()
            print(connection)
            attempts += 1
            time.sleep(1) # wait 1 second before retrying
    
    # if all retries failed, raise an exception
    raise Exception("Could not insert into database after multiple attempts.")



def delete_all_tables_from_list():
    
    tut_tables_list = ['`action-types`', 'adhoccharges', 'agents', '`ahc-categories`', 'appointments', 'branch', 'clients', '`contractor-availability`', 'contractors', 'enquiry', 'invoices', 'labels', '`payment-orders`', '`pipeline-stages`', '`proforma-invoices`', 'public_contractors', 'qual_levels', 'recipients', 'recipient_appointments', 'reports', 'reviews', 'services', '`skill-sets`', 'subjects', 'tenders']
    lesson_tables_list = ['`my-organisation`', 'sessions', 'spaces', 'users']
    # Loop through each table name in the list and attempt to delete it

    temp_tut_tables_list = ['`temp_action-temp_types`', 'temp_adhoccharges', 'temp_agents', '`temp_ahc-temp_categories`', 'temp_appointments', 'temp_branch', 'temp_clients', '`temp_contractor-temp_availability`', 'temp_contractors', 'temp_enquiry', 'temp_invoices', 'temp_labels', '`temp_payment-temp_orders`', '`temp_pipeline-temp_stages`', '`temp_proforma-temp_invoices`', 'temp_public_contractors', 'temp_qual_levels', 'temp_recipients', 'temp_recipient_appointments', 'temp_reports', 'temp_reviews', 'temp_services', '`temp_skill-temp_sets`', 'temp_subjects', 'temp_tenders']
    temp_lesson_tables_list = ['`temp_my-temp_organisation`', 'temp_sessions', 'temp_spaces', 'temp_users']

    all = [tut_tables_list, lesson_tables_list, temp_tut_tables_list, temp_lesson_tables_list]

    for a in all:
        for table_name in a:
            try:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                print(f"Table {table_name} deleted successfully")
            except Exception as e:
                print(f"Error deleting table {table_name}: {e}")
                connection.rollback()
                

        connection.commit()

# delete_all_tables_from_list()






def merge_tables(source_table, target_table):

    for col in source_table.columns:
        print(col)

    # query = f"""
    #     MERGE INTO {target_table} AS target
    #     USING (SELECT * FROM {source_table}) AS source
    #     ON (target.id = source.id)
    #     WHEN MATCHED THEN
    #         UPDATE SET {', '.join([f'target.{col}=source.{col}' for col in source_table.columns if col != 'id'])}
    #     WHEN NOT MATCHED THEN
    #         INSERT ({', '.join(source_table.columns)}) VALUES ({', '.join([f'source.{col}' for col in source_table.columns])})
    # """

    # cur.execute(query)
    # connection.commit()

# merge_tables("")


def get_id_from_contractor():
    cur.execute('''SELECT ca.id, ca.insert_time
FROM db0vuk9uhcblqc.contractors ca
WHERE ca.insert_time > (
    SELECT c.insert_time
    FROM db0vuk9uhcblqc.contractor_availability c
    ORDER BY c.insert_time DESC
    LIMIT 1
)''')
    result = cur.fetchall()  # fetch all rows as a list of tuples
    connection.commit()
    return result


def get_id_from_contractor_all():
    cur.execute('''SELECT ca.id, ca.insert_time
FROM db0vuk9uhcblqc.pb_contractors ca''')
    result = cur.fetchall()  # fetch all rows as a list of tuples
    connection.commit()
    return result








































# ------------------ un-used function ------ will use in furture to make process eff --------

# from sqlalchemy import create_engine, Table, Column, Integer, String, Boolean, TIMESTAMP, MetaData
# def insert_value(column_values, table_name, engine):
#     metadata = MetaData()
#     table = Table(table_name, metadata, autoload_with=engine)
    
#     # Coerce values into appropriate types
#     for column in table.columns:
#         value = column_values.get(column.name)
#         if isinstance(value, bool):
#             column_values[column.name] = int(value)
#         elif isinstance(value, str) and 'VARCHAR' in str(column.type):
#             column_values[column.name] = value[:column.type.length]
#         elif isinstance(value, int) and isinstance(column.type, Integer):
#             column_values[column.name] = int(value)
#         elif isinstance(value, str) and isinstance(column.type, TIMESTAMP):
#             column_values[column.name] = pd.to_datetime(value, errors='coerce')
    
#     conn = engine.connect()
#     try:
#         conn.execute(table.insert().values(column_values))
#     except:
#         metadata.create_all(engine)
#         conn.execute(table.insert().values(column_values))
#     conn.close()



# column_values = {
#     'created_time': '2023-02-20 10:30:00',
#     'sql_query': 'SELECT * FROM hello',
#     'table_name': 'mytable'
# }

# insert_value(column_values, 'mapping_metadata')

