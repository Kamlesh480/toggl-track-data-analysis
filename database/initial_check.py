import connect_database
import database_func

connction = connect_database.conn()
print(connction)

def check_mapping_metadata_table():
    # Check if the mapping_metadata table already exists
    if not database_func.is_table_exists('mapping_metadata'):
        # Create the mapping_metadata table
        query = '''
            CREATE TABLE mapping_metadata (
                id INT AUTO_INCREMENT PRIMARY KEY,
                created_time TIMESTAMP,
                sql_query TEXT,
                table_name VARCHAR(255)
            )
        '''
        database_func.create_table(query)
        print("created mapping_metadata table")
    else:
        print("mapping_metadata alredy there")

# check_mapping_metadata_table()