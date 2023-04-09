import sqlalchemy as db
from . import credentials

def generate_table_query_from_json(data, table_name):
    metadata = db.MetaData()

    columns = []
    has_id_column = False
    for key, value in data.items():
        # print(key)
        # print(type(value))
        if value is None:
            # Handle null values
            column = db.Column(key, db.TEXT)
        elif isinstance(value, str):
            # Handle string values
            if key == "id":
                column = db.Column(key, db.String(512))
            else:
                column = db.Column(key, db.TEXT)
        elif isinstance(value, bool):
            # Handle boolean values
            column = db.Column(key, db.Boolean)
        elif isinstance(value, int):
            # Handle integer values
            column = db.Column(key, db.Integer)
        else:
            # Handle other types as strings
            if key == "id":
                column = db.Column(key, db.String(1024))
            else:
                column = db.Column(key, db.TEXT)
        if key == "id":
            column.primary_key = True
            has_id_column = True
        columns.append(column)

    # Add columns for insertion time and hash key
    insert_time_column = db.Column('insert_time', db.TIMESTAMP, nullable=False)
    if not has_id_column:
        hash_key_column = db.Column('pk_hash_key', db.String(64), primary_key=False)
        columns.append(hash_key_column)
    columns.append(insert_time_column)

    table = db.Table(table_name, metadata, *columns)

    # Generate the SQL query for creating the table
    query = db.schema.CreateTable(table).compile(db.create_engine('mysql+pymysql://'+ credentials.username +':'+ credentials.password +'@'+ credentials.host +'/' + credentials.database + ''))

    return str(query)


